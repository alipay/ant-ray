// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/object_manager/object_manager.h"

#include <chrono>

#include "ray/common/common_protocol.h"
#include "ray/stats/stats.h"
#include "ray/util/util.h"

namespace asio = boost::asio;

namespace ray {

ObjectStoreRunner::ObjectStoreRunner(const ObjectManagerConfig &config,
                                     SpillObjectsCallback spill_objects_callback,
                                     std::function<void()> object_store_full_callback,
                                     AddObjectCallback add_object_callback,
                                     DeleteObjectCallback delete_object_callback) {
  plasma::plasma_store_runner.reset(
      new plasma::PlasmaStoreRunner(config.store_socket_name, config.object_store_memory,
                                    config.huge_pages, config.plasma_directory));
  // Initialize object store.
  store_thread_ =
      std::thread(&plasma::PlasmaStoreRunner::Start, plasma::plasma_store_runner.get(),
                  spill_objects_callback, object_store_full_callback, add_object_callback,
                  delete_object_callback);
  // Sleep for sometime until the store is working. This can suppress some
  // connection warnings.
  std::this_thread::sleep_for(std::chrono::microseconds(500));
}

ObjectStoreRunner::~ObjectStoreRunner() {
  plasma::plasma_store_runner->Stop();
  store_thread_.join();
  plasma::plasma_store_runner.reset();
}

ObjectManager::ObjectManager(
    instrumented_io_context &main_service, const NodeID &self_node_id,
    const ObjectManagerConfig &config,
    std::shared_ptr<ObjectDirectoryInterface> object_directory,
    RestoreSpilledObjectCallback restore_spilled_object,
    std::function<std::string(const ObjectID &)> get_spilled_object_url,
    SpillObjectsCallback spill_objects_callback,
    std::function<void()> object_store_full_callback,
    AddObjectCallback add_object_callback, DeleteObjectCallback delete_object_callback)
    : main_service_(&main_service),
      self_node_id_(self_node_id),
      config_(config),
      object_directory_(std::move(object_directory)),
      object_store_internal_(
          config, spill_objects_callback, object_store_full_callback,
          /*add_object_callback=*/
          [this, add_object_callback =
                     std::move(add_object_callback)](const ObjectInfo &object_info) {
            main_service_->post(
                [this, object_info,
                 add_object_callback = std::move(add_object_callback)]() {
                  HandleObjectAdded(object_info);
                  add_object_callback(object_info);
                },
                "ObjectManager.ObjectAdded");
          },
          /*delete_object_callback=*/
          [this, delete_object_callback =
                     std::move(delete_object_callback)](const ObjectID &object_id) {
            main_service_->post(
                [this, object_id,
                 delete_object_callback = std::move(delete_object_callback)]() {
                  HandleObjectDeleted(object_id);
                  delete_object_callback(object_id);
                },
                "ObjectManager.ObjectDeleted");
          }),
      buffer_pool_(config_.store_socket_name, config_.object_chunk_size),
      rpc_work_(rpc_service_),
      object_manager_server_("ObjectManager", config_.object_manager_port,
                             config_.rpc_service_threads_number),
      object_manager_service_(rpc_service_, *this),
      // client_call_manager_(main_service, config_.rpc_service_threads_number),
      client_call_manager_(rpc_service_),
      restore_spilled_object_(restore_spilled_object),
      get_spilled_object_url_(get_spilled_object_url),
      pull_retry_timer_(*main_service_,
                        boost::posix_time::milliseconds(config.timer_freq_ms)) {
  RAY_CHECK(config_.rpc_service_threads_number > 0);

  push_manager_.reset(new PushManager(/* max_chunks_in_flight= */ std::max(
      static_cast<int64_t>(1L),
      static_cast<int64_t>(config_.max_bytes_in_flight / config_.object_chunk_size))));

  pull_retry_timer_.async_wait([this](const boost::system::error_code &e) { Tick(e); });

  const auto &object_is_local = [this](const ObjectID &object_id) {
    return local_objects_.count(object_id) != 0;
  };
  const auto &send_pull_request = [this](const ObjectID &object_id,
                                         const NodeID &client_id) {
    SendPullRequest(object_id, client_id);
  };
  const auto &cancel_pull_request = [this](const ObjectID &object_id) {
    // We must abort this object because it may have only been partially
    // created and will cause a leak if we never receive the rest of the
    // object. This is a no-op if the object is already sealed or evicted.
    buffer_pool_.AbortCreate(object_id);
  };
  const auto &get_time = []() { return absl::GetCurrentTimeNanos() / 1e9; };
  int64_t available_memory = config.object_store_memory;
  if (available_memory < 0) {
    available_memory = 0;
  }
  pull_manager_.reset(new PullManager(
      self_node_id_, object_is_local, send_pull_request, cancel_pull_request,
      restore_spilled_object_, get_time, config.pull_timeout_ms, available_memory,
      [spill_objects_callback, object_store_full_callback]() {
        // TODO(swang): This copies the out-of-memory handling in the
        // CreateRequestQueue. It would be nice to unify these.
        object_store_full_callback();
        static_cast<void>(spill_objects_callback());
      }));
  // Start object manager rpc server and send & receive request threads
  StartRpcService();
}

ObjectManager::~ObjectManager() { StopRpcService(); }

void ObjectManager::Stop() { plasma::plasma_store_runner->Stop(); }

bool ObjectManager::IsPlasmaObjectSpillable(const ObjectID &object_id) {
  return plasma::plasma_store_runner->IsPlasmaObjectSpillable(object_id);
}

void ObjectManager::RunRpcService(int index) {
  SetThreadName("rpc.obj.mgr." + std::to_string(index));
  rpc_service_.run();
}

void ObjectManager::StartRpcService() {
  rpc_threads_.resize(config_.rpc_service_threads_number);
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i] = std::thread(&ObjectManager::RunRpcService, this, i);
  }
  object_manager_server_.RegisterStreamService(object_manager_service_);
  object_manager_server_.Run();
}

void ObjectManager::StopRpcService() {
  rpc_service_.stop();
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i].join();
  }
  object_manager_server_.Shutdown();
}

void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  // Notify the object directory that the object has been added to this node.
  const ObjectID &object_id = object_info.object_id;
  RAY_LOG(DEBUG) << "Object added " << object_id;
  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;
  ray::Status status =
      object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);

  // Handle the unfulfilled_push_requests_ which contains the push request that is not
  // completed due to unsatisfied local objects.
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      auto &node_id = pair.first;
      main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); },
                          "ObjectManager.ObjectAddedPush");
      // When push timeout is set to -1, there will be an empty timer in pair.second.
      if (pair.second != nullptr) {
        pair.second->cancel();
      }
    }
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  RAY_CHECK(it != local_objects_.end());
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;
  RAY_CHECK(!local_objects_.empty() || used_memory_ == 0);
  ray::Status status =
      object_directory_->ReportObjectRemoved(object_id, self_node_id_, object_info);

  // Ask the pull manager to fetch this object again as soon as possible, if
  // it was needed by an active pull request.
  pull_manager_->ResetRetryTimer(object_id);
}

uint64_t ObjectManager::Pull(const std::vector<rpc::ObjectReference> &object_refs,
                             bool is_worker_request) {
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto request_id =
      pull_manager_->Pull(object_refs, is_worker_request, &objects_to_locate);

  const auto &callback = [this](const ObjectID &object_id,
                                const std::unordered_set<NodeID> &client_ids,
                                const std::string &spilled_url,
                                const NodeID &spilled_node_id, size_t object_size) {
    pull_manager_->OnLocationChange(object_id, client_ids, spilled_url, spilled_node_id,
                                    object_size);
  };

  for (const auto &ref : objects_to_locate) {
    // Subscribe to object notifications. A notification will be received every
    // time the set of node IDs for the object changes. Notifications will also
    // be received if the list of locations is empty. The set of node IDs has
    // no ordering guarantee between notifications.
    auto object_id = ObjectRefToId(ref);
    RAY_CHECK_OK(object_directory_->SubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id, ref.owner_address(), callback));
  }

  return request_id;
}

void ObjectManager::CancelPull(uint64_t request_id) {
  const auto objects_to_cancel = pull_manager_->CancelPull(request_id);
  for (const auto &object_id : objects_to_cancel) {
    RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id));
  }
}

void ObjectManager::SendPullRequest(const ObjectID &object_id, const NodeID &client_id) {
  auto rpc_client = GetRpcClient(client_id);
  if (rpc_client) {
    // Try pulling from the client.
    RAY_LOG(WARNING) << "[RDMA][Puller][Object Pull RTT] Start send pull request for object: " << object_id
    << " to client: " << client_id;
    rpc_service_.post(
        [this, object_id, client_id, rpc_client=std::move(rpc_client)]() {
          rpc::PullRequest pull_request;
          pull_request.set_object_id(object_id.Binary());
          pull_request.set_node_id(self_node_id_.Binary());

          rpc_client->Pull(
              pull_request,
              [object_id, client_id](const Status &status, const rpc::PullReply &reply) {
                if (!status.ok()) {
                  RAY_LOG(WARNING) << "Send pull " << object_id << " request to client "
                                   << client_id << " failed due to" << status.message();
                }
              });
        },
        "ObjectManager.SendPull");
  } else {
    RAY_LOG(ERROR) << "Couldn't send pull request from " << self_node_id_ << " to "
                   << client_id << " of object " << object_id
                   << " , setup rpc connection failed.";
  }
}

void ObjectManager::HandlePushTaskTimeout(const ObjectID &object_id,
                                          const NodeID &node_id) {
  RAY_LOG(WARNING) << "Invalid Push request ObjectID: " << object_id
                   << " after waiting for " << config_.push_timeout_ms << " ms.";
  auto iter = unfulfilled_push_requests_.find(object_id);
  // Under this scenario, `HandlePushTaskTimeout` can be invoked
  // although timer cancels it.
  // 1. wait timer is done and the task is queued.
  // 2. While task is queued, timer->cancel() is invoked.
  // In this case this method can be invoked although it is not timed out.
  // https://www.boost.org/doc/libs/1_66_0/doc/html/boost_asio/reference/basic_deadline_timer/cancel/overload1.html.
  if (iter == unfulfilled_push_requests_.end()) {
    return;
  }
  size_t num_erased = iter->second.erase(node_id);
  RAY_CHECK(num_erased == 1);
  if (iter->second.size() == 0) {
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleSendFinished(const ObjectID &object_id, const NodeID &node_id,
                                       uint64_t chunk_index, double start_time,
                                       double end_time, ray::Status status) {
  RAY_LOG(DEBUG) << "HandleSendFinished on " << self_node_id_ << " to " << node_id
                 << " of object " << object_id << " chunk " << chunk_index
                 << ", status: " << status.ToString();
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
  }

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("transfer_send");
  profile_event.set_start_time(start_time);
  profile_event.set_end_time(end_time);
  // Encode the object ID, node ID, chunk index, and status as a json list,
  // which will be parsed by the reader of the profile table.
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + node_id.Hex() + "\"," +
                               std::to_string(chunk_index) + ",\"" + status.ToString() +
                               "\"]");
  RAY_LOG(WARNING) << "[RDMA][Pusher][Chunk RTT] Receive object chunk, object: " << object_id
  << " , chunk index: " << chunk_index;
  // std::lock_guard<std::mutex> lock(profile_mutex_);
  // profile_events_.push_back(profile_event);
}

void ObjectManager::HandleReceiveFinished(const ObjectID &object_id,
                                          const NodeID &node_id, uint64_t chunk_index,
                                          double start_time, double end_time) {
  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("transfer_receive");
  profile_event.set_start_time(start_time);
  profile_event.set_end_time(end_time);
  // Encode the object ID, node ID, chunk index as a json list,
  // which will be parsed by the reader of the profile table.
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + node_id.Hex() + "\"," +
                               std::to_string(chunk_index) + "]");
  // std::lock_guard<std::mutex> lock(profile_mutex_);
  // profile_events_.push_back(profile_event);
}

void ObjectManager::Push(const ObjectID &object_id, const NodeID &node_id) {
  RAY_LOG(DEBUG) << "Push on " << self_node_id_ << " to " << node_id << " of object "
                 << object_id;
  if (local_objects_.count(object_id) != 0) {
    return PushLocalObject(object_id, node_id);
  }

  // Push from spilled object directly if the object is on local disk.
  auto object_url = get_spilled_object_url_(object_id);
  if (!object_url.empty() && RayConfig::instance().is_external_storage_type_fs()) {
    return PushFromFilesystem(object_id, node_id, object_url);
  }

  // Avoid setting duplicated timer for the same object and node pair.
  auto &nodes = unfulfilled_push_requests_[object_id];

  if (nodes.count(node_id) == 0) {
    // If config_.push_timeout_ms < 0, we give an empty timer
    // and the task will be kept infinitely.
    std::unique_ptr<boost::asio::deadline_timer> timer;
    if (config_.push_timeout_ms == 0) {
      // The Push request fails directly when config_.push_timeout_ms == 0.
      RAY_LOG(WARNING) << "Invalid Push request ObjectID " << object_id
                       << " due to direct timeout setting. (0 ms timeout)";
    } else if (config_.push_timeout_ms > 0) {
      // Put the task into a queue and wait for the notification of Object added.
      timer.reset(new boost::asio::deadline_timer(*main_service_));
      auto clean_push_period = boost::posix_time::milliseconds(config_.push_timeout_ms);
      timer->expires_from_now(clean_push_period);
      timer->async_wait(
          [this, object_id, node_id](const boost::system::error_code &error) {
            // Timer killing will receive the boost::asio::error::operation_aborted,
            // we only handle the timeout event.
            if (!error) {
              HandlePushTaskTimeout(object_id, node_id);
            }
          });
    }
    if (config_.push_timeout_ms != 0) {
      nodes.emplace(node_id, std::move(timer));
    }
  }
}

void ObjectManager::PushLocalObject(const ObjectID &object_id, const NodeID &node_id) {
  const ObjectInfo &object_info = local_objects_[object_id].object_info;
  uint64_t total_data_size =
      static_cast<uint64_t>(object_info.data_size + object_info.metadata_size);
  uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);
  uint64_t num_chunks = buffer_pool_.GetNumChunks(total_data_size);

  rpc::Address owner_address;
  owner_address.set_raylet_id(object_info.owner_raylet_id.Binary());
  owner_address.set_ip_address(object_info.owner_ip_address);
  owner_address.set_port(object_info.owner_port);
  owner_address.set_worker_id(object_info.owner_worker_id.Binary());

  auto local_chunk_reader = [this, object_id, total_data_size, metadata_size](
                                uint64_t chunk_index,
                                rpc::PushRequest &push_request) -> Status {
    std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
        buffer_pool_.GetChunk(object_id, total_data_size, metadata_size, chunk_index);
    // Fail on status not okay. The object is local, and there is
    // no other anticipated error here.
    Status status = chunk_status.second;
    if (status.ok()) {
      ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;
      push_request.set_data(chunk_info.data, chunk_info.buffer_length);
    }
    return status;
  };

  auto release_chunk_callback = [this, object_id](uint64_t chunk_index) {
    buffer_pool_.ReleaseGetChunk(object_id, chunk_index);
  };

  PushObjectInternal(object_id, node_id, total_data_size, metadata_size, num_chunks,
                     std::move(owner_address), std::move(local_chunk_reader),
                     std::move(release_chunk_callback));
}

void ObjectManager::PushFromFilesystem(const ObjectID &object_id, const NodeID &node_id,
                                       const std::string &spilled_url) {
  // SpilledObject::CreateSpilledObject does synchronous IO; schedule it off
  // main thread.
  rpc_service_.post(
      [this, object_id, node_id, spilled_url, chunk_size = config_.object_chunk_size]() {
        auto optional_spilled_object =
            SpilledObject::CreateSpilledObject(spilled_url, chunk_size);
        if (!optional_spilled_object.has_value()) {
          RAY_LOG(ERROR) << "Failed to load spilled object " << object_id
                         << ". It may have been evicted.";
          return;
        }

        auto spilled_object =
            std::make_shared<SpilledObject>(std::move(optional_spilled_object.value()));

        uint64_t total_data_size =
            spilled_object->GetDataSize() + spilled_object->GetMetadataSize();
        uint64_t metadata_size = spilled_object->GetMetadataSize();
        uint64_t num_chunks = spilled_object->GetNumChunks();
        rpc::Address owner_address = spilled_object->GetOwnerAddress();

        auto spilled_object_chunk_reader = [object_id, spilled_object](
                                               uint64_t chunk_index,
                                               rpc::PushRequest &push_request) -> Status {
          auto optional_chunk = spilled_object->GetChunk(chunk_index);
          if (!optional_chunk.has_value()) {
            RAY_LOG(ERROR) << "Read chunk " << chunk_index << " of object " << object_id
                           << " failed. "
                           << " It may have been evicted.";
            return Status::IOError("Failed to read spilled object");
          }
          push_request.set_data(std::move(optional_chunk.value()));
          return Status::OK();
        };

        // Schedule PushObjectInternal back to main_service as PushObjectInternal access
        // thread unsafe datastructure.
        main_service_->post(
            [this, object_id, node_id, total_data_size, metadata_size, num_chunks,
             owner_address = std::move(owner_address),
             spilled_object_chunk_reader = std::move(spilled_object_chunk_reader)]() {
              PushObjectInternal(object_id, node_id, total_data_size, metadata_size,
                                 num_chunks, std::move(owner_address),
                                 std::move(spilled_object_chunk_reader),
                                 [](uint64_t) { /* do nothing to release chunk */ });
            },
            "ObjectManager.PushLocalSpilledObjectInternal");
      },
      "ObjectManager.CreateSpilledObject");
}

void ObjectManager::PushObjectInternal(
    const ObjectID &object_id, const NodeID &node_id, uint64_t total_data_size,
    uint64_t metadata_size, uint64_t num_chunks, rpc::Address owner_address,
    std::function<ray::Status(uint64_t, rpc::PushRequest &)> chunk_reader,
    std::function<void(uint64_t)> release_chunk_callback) {
  auto rpc_client = GetRpcClient(node_id);
  if (!rpc_client) {
    // Push is best effort, so do nothing here.
    RAY_LOG(ERROR)
        << "Failed to establish connection for Push with remote object manager.";
    return;
  }

  RAY_LOG(DEBUG) << "Sending object chunks of " << object_id << " to node " << node_id
                 << ", number of chunks: " << num_chunks
                 << ", total data size: " << total_data_size;

  auto push_id = UniqueID::FromRandom();
  push_manager_->StartPush(node_id, object_id, num_chunks, [=](int64_t chunk_id) {
    rpc_service_.post(
        [=]() {
          // Post to the multithreaded RPC event loop so that data is copied
          // off of the main thread.
          SendObjectChunk(push_id, object_id, owner_address, node_id, total_data_size,
                          metadata_size, chunk_id, rpc_client,
                          [=](const Status &status) {
                            // Post back to the main event loop because the
                            // PushManager is thread-safe.
                            main_service_->post(
                                [this, node_id, object_id]() {
                                  push_manager_->OnChunkComplete(node_id, object_id);
                                },
                                "ObjectManager.Push");
                          },
                          std::move(chunk_reader), std::move(release_chunk_callback));
        },
        "ObjectManager.Push");
  });
}

void ObjectManager::SendObjectChunk(
    const UniqueID &push_id, const ObjectID &object_id, const rpc::Address &owner_address,
    const NodeID &node_id, uint64_t total_data_size, uint64_t metadata_size,
    uint64_t chunk_index, std::shared_ptr<rpc::ObjectManagerBrpcClients> rpc_client,
    std::function<void(const Status &)> on_complete,
    std::function<ray::Status(uint64_t, rpc::PushRequest &)> chunk_reader,
    std::function<void(uint64_t)> release_chunk_callback) {
  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  rpc::PushRequest push_request;
  // Set request header
  push_request.set_push_id(push_id.Binary());
  push_request.set_object_id(object_id.Binary());
  push_request.mutable_owner_address()->CopyFrom(owner_address);
  push_request.set_node_id(self_node_id_.Binary());
  push_request.set_data_size(total_data_size);
  push_request.set_metadata_size(metadata_size);
  push_request.set_chunk_index(chunk_index);

  // read a chunk into push_request and handle errors.
  auto status = chunk_reader(chunk_index, push_request);
  if (!status.ok()) {
    RAY_LOG(WARNING) << "Attempting to push object " << object_id
                     << " which is not local. It may have been evicted.";
    on_complete(status);
    return;
  }

  // record the time cost between send chunk and receive reply
  rpc::ClientCallback<rpc::PushReply> callback =
      [this, start_time, object_id, node_id, chunk_index, owner_address, rpc_client,
       on_complete](const Status &status, const rpc::PushReply &reply) {
        // TODO: Just print warning here, should we try to resend this chunk?
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Send object " << object_id << " chunk to node " << node_id
                           << " failed due to" << status.message()
                           << ", chunk index: " << chunk_index;
        }
        double end_time = absl::GetCurrentTimeNanos() / 1e9;
        HandleSendFinished(object_id, node_id, chunk_index, start_time, end_time, status);
        on_complete(status);
      };

  if (chunk_index == 0) {
    // The first chunk of current object
    RAY_LOG(WARNING) << "[RDMA][Pusher][Object Transfer RTT] Start send first chunk of object " << object_id;
  }
  RAY_LOG(WARNING) << "[RDMA][Pusher][Chunk RTT] Send object chunk, object: " << object_id
    << " , chunk index: " << chunk_index;
  rpc_client->Push(push_request, callback);

  release_chunk_callback(chunk_index);
}

/// Implementation of ObjectManagerServiceHandler
void ObjectManager::HandlePush(const rpc::PushRequest &request, rpc::PushReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());

  // Serialize.
  uint64_t chunk_index = request.chunk_index();
  uint64_t metadata_size = request.metadata_size();
  uint64_t data_size = request.data_size();
  const rpc::Address &owner_address = request.owner_address();
  const std::string &data = request.data();

  RAY_LOG(WARNING) << "[RDMA][Puller][Chunk RTT] Receive object chunk, object: " << object_id
    << " , chunk index: " << chunk_index;

  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  bool success = ReceiveObjectChunk(node_id, object_id, owner_address, data_size,
                                    metadata_size, chunk_index, data);
  if (!success) {
    num_chunks_received_failed_++;
    RAY_LOG(INFO) << "Received duplicate or cancelled chunk at index " << chunk_index
                  << " of object " << object_id << ": overall "
                  << num_chunks_received_failed_ << "/" << num_chunks_received_total_
                  << " failed";
  }
  double end_time = absl::GetCurrentTimeNanos() / 1e9;

  HandleReceiveFinished(object_id, node_id, chunk_index, start_time, end_time);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

bool ObjectManager::ReceiveObjectChunk(const NodeID &node_id, const ObjectID &object_id,
                                       const rpc::Address &owner_address,
                                       uint64_t data_size, uint64_t metadata_size,
                                       uint64_t chunk_index, const std::string &data) {
  num_chunks_received_total_++;
  RAY_LOG(DEBUG) << "ReceiveObjectChunk on " << self_node_id_ << " from " << node_id
                 << " of object " << object_id << " chunk index: " << chunk_index
                 << ", chunk data size: " << data.size()
                 << ", object size: " << data_size;

  if (!pull_manager_->IsObjectActive(object_id)) {
    // This object is no longer being actively pulled. Do not create the object.
    return false;
  }
  std::pair<const ObjectBufferPool::ChunkInfo &, ray::Status> chunk_status =
      buffer_pool_.CreateChunk(object_id, owner_address, data_size, metadata_size,
                               chunk_index);
  if (!pull_manager_->IsObjectActive(object_id)) {
    // This object is no longer being actively pulled. Abort the object. We
    // have to check again here because the pull manager runs in a different
    // thread and the object may have been deactivated right before creating
    // the chunk.
    buffer_pool_.AbortCreate(object_id);
    return false;
  }

  ObjectBufferPool::ChunkInfo chunk_info = chunk_status.first;
  if (chunk_status.second.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    std::memcpy(chunk_info.data, data.data(), chunk_info.buffer_length);
    buffer_pool_.SealChunk(object_id, chunk_index);
    return true;
  } else {
    RAY_LOG(INFO) << "Error receiving chunk:" << chunk_status.second.message();
    return false;
  }
}

void ObjectManager::HandlePull(const rpc::PullRequest &request, rpc::PullReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(WARNING) << "[RDMA][Pusher][Object Push RTT] Received pull request from node " << node_id << " for object "
                << object_id;
  RAY_LOG(DEBUG) << "Received pull request from node " << node_id << " for object ["
                 << object_id << "].";

  rpc::ProfileTableData::ProfileEvent profile_event;
  profile_event.set_event_type("receive_pull_request");
  profile_event.set_start_time(absl::GetCurrentTimeNanos() / 1e9);
  profile_event.set_end_time(profile_event.start_time());
  profile_event.set_extra_data("[\"" + object_id.Hex() + "\",\"" + node_id.Hex() + "\"]");
  // {
  //   std::lock_guard<std::mutex> lock(profile_mutex_);
  //   profile_events_.emplace_back(profile_event);
  // }

  main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); },
                      "ObjectManager.HandlePull");
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::HandleFreeObjects(const rpc::FreeObjectsRequest &request,
                                      rpc::FreeObjectsReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  for (const auto &e : request.object_ids()) {
    object_ids.emplace_back(ObjectID::FromBinary(e));
  }
  FreeObjects(object_ids, /* local_only */ true);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::FreeObjects(const std::vector<ObjectID> &object_ids,
                                bool local_only) {
  buffer_pool_.FreeObjects(object_ids);
  if (!local_only) {
    const auto remote_connections = object_directory_->LookupAllRemoteConnections();
    std::vector<std::shared_ptr<rpc::ObjectManagerBrpcClients>> rpc_clients;
    for (const auto &connection_info : remote_connections) {
      auto rpc_client = GetRpcClient(connection_info.node_id);
      if (rpc_client != nullptr) {
        rpc_clients.push_back(rpc_client);
      }
    }
    rpc_service_.post(
        [this, object_ids, rpc_clients]() {
          SpreadFreeObjectsRequest(object_ids, rpc_clients);
        },
        "ObjectManager.FreeObjects");
  }
}

void ObjectManager::SpreadFreeObjectsRequest(
    const std::vector<ObjectID> &object_ids,
    const std::vector<std::shared_ptr<rpc::ObjectManagerBrpcClients>> &rpc_clients) {
  // This code path should be called from node manager.
  rpc::FreeObjectsRequest free_objects_request;
  for (const auto &e : object_ids) {
    free_objects_request.add_object_ids(e.Binary());
  }
  for (const auto &rpc_client : rpc_clients) {
    rpc_client->FreeObjects(free_objects_request, [](const Status &status,
                                                     const rpc::FreeObjectsReply &reply) {
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Send free objects request failed due to" << status.message();
      }
    });
  }
}

std::shared_ptr<rpc::ObjectManagerBrpcClients> ObjectManager::GetRpcClient(
    const NodeID &node_id) {
  auto it = remote_object_manager_clients_.find(node_id);
  if (it == remote_object_manager_clients_.end()) {
    RemoteConnectionInfo connection_info(node_id);
    object_directory_->LookupRemoteConnectionInfo(connection_info);
    if (!connection_info.Connected()) {
      return nullptr;
    }
    auto object_manager_client = std::make_shared<rpc::ObjectManagerBrpcClients>(
        connection_info.ip, connection_info.port, client_call_manager_, RayConfig::instance().object_manager_conn_num());

    RAY_LOG(DEBUG) << "Get rpc client, address: " << connection_info.ip
                   << ", port: " << connection_info.port
                   << ", local port: " << GetServerPort();

    it = remote_object_manager_clients_.emplace(node_id, std::move(object_manager_client))
             .first;
  }
  return it->second;
}

std::shared_ptr<rpc::ProfileTableData> ObjectManager::GetAndResetProfilingInfo() {
  auto profile_info = std::make_shared<rpc::ProfileTableData>();
  profile_info->set_component_type("object_manager");
  profile_info->set_component_id(self_node_id_.Binary());

  // {
  //   std::lock_guard<std::mutex> lock(profile_mutex_);
  //   for (auto const &profile_event : profile_events_) {
  //     profile_info->add_profile_events()->CopyFrom(profile_event);
  //   }
  //   profile_events_.clear();
  // }

  return profile_info;
}

std::string ObjectManager::DebugString() const {
  std::stringstream result;
  result << "ObjectManager:";
  result << "\n- num local objects: " << local_objects_.size();
  result << "\n- num unfulfilled push requests: " << unfulfilled_push_requests_.size();
  result << "\n- num pull requests: " << pull_manager_->NumActiveRequests();
  result << "\n- num buffered profile events: " << profile_events_.size();
  result << "\n- num chunks received total: " << num_chunks_received_total_;
  result << "\n- num chunks received failed: " << num_chunks_received_failed_;
  result << "\nEvent loop stats:" << rpc_service_.StatsString();
  result << "\n" << push_manager_->DebugString();
  result << "\n" << object_directory_->DebugString();
  result << "\n" << buffer_pool_.DebugString();
  result << "\n" << pull_manager_->DebugString();
  return result.str();
}

void ObjectManager::RecordMetrics() const {
  stats::ObjectStoreAvailableMemory().Record(config_.object_store_memory - used_memory_);
  stats::ObjectStoreUsedMemory().Record(used_memory_);
  stats::ObjectStoreLocalObjects().Record(local_objects_.size());
  stats::ObjectManagerPullRequests().Record(pull_manager_->NumActiveRequests());
}

void ObjectManager::FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_object_store_bytes_used(used_memory_);
  stats->set_object_store_bytes_avail(config_.object_store_memory);
  stats->set_num_local_objects(local_objects_.size());
  stats->set_consumed_bytes(plasma::plasma_store_runner->GetConsumedBytes());
}

void ObjectManager::Tick(const boost::system::error_code &e) {
  RAY_CHECK(!e) << "The raylet's object manager has failed unexpectedly with error: " << e
                << ". Please file a bug report on here: "
                   "https://github.com/ray-project/ray/issues";

  // Request the current available memory from the object
  // store.
  plasma::plasma_store_runner->GetAvailableMemoryAsync([this](size_t available_memory) {
    main_service_->post(
        [this, available_memory]() {
          pull_manager_->UpdatePullsBasedOnAvailableMemory(available_memory);
        },
        "ObjectManager.UpdateAvailableMemory");
  });

  pull_manager_->Tick();

  auto interval = boost::posix_time::milliseconds(config_.timer_freq_ms);
  pull_retry_timer_.expires_from_now(interval);
  pull_retry_timer_.async_wait([this](const boost::system::error_code &e) { Tick(e); });
}

}  // namespace ray
