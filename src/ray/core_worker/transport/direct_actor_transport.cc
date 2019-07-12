
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/common/task/task.h"

using ray::rpc::ActorTableData;

namespace ray {

bool HasByReferenceArgs(const TaskSpecification &spec) {
  for (int i = 0; i < spec.NumArgs(); ++i) {
    if (spec.ArgIdCount(i) > 0) {
      return true;
    }
  }
  return false;
}

CoreWorkerDirectActorTaskSubmitter::CoreWorkerDirectActorTaskSubmitter(
    boost::asio::io_service &io_service,
    gcs::GcsClient &gcs_client,
    CoreWorkerObjectInterface &object_interface)
    : io_service_(io_service),
      gcs_client_(gcs_client),
      client_call_manager_(io_service),
      store_provider_(object_interface.CreateStoreProvider(StoreProviderType::LOCAL_PLASMA)),
      counter_(0) {
       
  RAY_CHECK_OK(SubscribeActorTable());
}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(const TaskSpec &task) {

  if (HasByReferenceArgs(task.GetTaskSpecification())) {
    return Status::Invalid("direct actor call only supports by value arguments");
  }

  RAY_CHECK(task.GetTaskSpecification().IsActorTask());
  const auto &actor_id = task.GetTaskSpecification().ActorId();

  const auto task_id = task.GetTaskSpecification().TaskId();
  const auto num_returns = task.GetTaskSpecification().NumReturns();

  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  request->set_task_id(task.GetTaskSpecification().TaskId().Binary());
  request->set_task_spec(task.GetTaskSpecification().Serialize());

  std::unique_lock<std::mutex> guard(rpc_clients_mutex_);
  auto entry = rpc_clients_.find(actor_id);
  if (entry == rpc_clients_.end()) {
    // TODO: what if actor is not created yet?
    // if (pending_requests_[actor_id].empty()) {
    //   gcs_client_.Actors().RequestNotifications(JobID::Nil(), actor_id,
    //      gcs_client_.GetLocalClientID());
    // }

    auto pending_request =
        std::unique_ptr<PendingTaskRequest>(new PendingTaskRequest);
    pending_request->task_id = task_id;
    pending_request->num_returns = num_returns;
    pending_request->request = std::move(request);
    // append the task to the pending list.

    pending_requests_[actor_id].emplace_back(std::move(pending_request));

    return Status::OK();
  }

  auto iter = actor_state_.find(actor_id);
  RAY_CHECK(iter != actor_state_.end());
  if (iter->second != ActorTableData::ALIVE) {
    TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
    return Status::IOError("actor is dead or being reconstructed");
  }

  auto &client = entry->second;
  RAY_LOG(DEBUG) << "push task " << "   " << task_id << "    " << client.get();  
  auto status = PushTask(*client, *request, task_id, num_returns);

  return status;
}

Status CoreWorkerDirectActorTaskSubmitter::SubscribeActorTable() {
  // Register a callback to handle actor notifications.
  auto actor_notification_callback = [this](const ActorID &actor_id,
                                            const std::vector<ActorTableData> &data) {
    if (!data.empty()) {
      const auto &actor_data = data.back();
      
      if (actor_data.state() == ActorTableData::ALIVE) {
        RAY_LOG(INFO) << "received notification on actor alive, actor_id: " << actor_id
                      << ", ip address: " << actor_data.ip_address()
                      << ", port: " << actor_data.port();

        std::unique_ptr<rpc::DirectActorClient> grpc_client(
            new rpc::DirectActorClient(actor_data.ip_address(),
            actor_data.port(), client_call_manager_));

        std::unique_lock<std::mutex> guard(rpc_clients_mutex_);
        actor_state_[actor_id] = actor_data.state();
        // replace old rpc client if it exists.
        rpc_clients_[actor_id] = std::move(grpc_client);

        auto entry = rpc_clients_.find(actor_id);
        RAY_CHECK(entry != rpc_clients_.end());

        auto &client = entry->second;
        auto &requests = pending_requests_[actor_id];
        while (!requests.empty()) {
          const auto &request = *requests.front();
          RAY_LOG(DEBUG) << "push pending task " << "   " << request.task_id << "    " << client.get();
          auto status = PushTask(*client, *request.request, request.task_id, request.num_returns);
          requests.pop_front();
        }
        
      } else if (actor_data.state() == ActorTableData::DEAD) {
        RAY_LOG(INFO) << "received notification on actor dead, actor_id: " << actor_id;

        std::unique_lock<std::mutex> guard(rpc_clients_mutex_);
        actor_state_[actor_id] = actor_data.state();

        // There are a couple of cases for actor/objects:
        // - dead
        // - reconstruction
        // - eviction
        // For get, there are a few possibilities:
        // - pending
        // - future (but on old objects)

      } else {
        //
        RAY_CHECK(actor_data.state() == ActorTableData::RECONSTRUCTING);
        RAY_LOG(INFO) << "received notification on actor reconstruction, actor_id: " << actor_id;

        std::unique_lock<std::mutex> guard(rpc_clients_mutex_);
        actor_state_[actor_id] = actor_data.state();
      }

      // TODO: handle other states.
    }
  };

  return gcs_client_.Actors().AsyncSubscribe(
      JobID::Nil(), ClientID::Nil(), actor_notification_callback, nullptr);
}

Status CoreWorkerDirectActorTaskSubmitter::PushTask(rpc::DirectActorClient &client,
    const rpc::PushTaskRequest &request, const TaskID &task_id, int num_returns) {
  auto status = client.PushTask(request, [this, task_id, num_returns](
                            Status status, const rpc::PushTaskReply &reply) {
    if (!status.ok()) {
      TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
      return;  
    }

    // TODO(zhijunfu): if return id count doesn't match, write an exception into store.
    RAY_CHECK(reply.return_object_ids_size() == reply.return_objects_size());
    for (int i = 0; i < reply.return_object_ids_size(); i++) {
      ObjectID object_id = ObjectID::FromBinary(reply.return_object_ids(i));
      const std::string &return_object = reply.return_objects(i);
      auto data = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(
          return_object.data()));
      auto buffer = std::make_shared<LocalMemoryBuffer>(data, return_object.size());
      store_provider_->Put(RayObject(buffer, nullptr), object_id);     
    }});
  return status;
}

void CoreWorkerDirectActorTaskSubmitter::TreatTaskAsFailed(
    const TaskID &task_id, int num_returns, const rpc::ErrorType &error_type) {

  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::ForTaskReturn(task_id, i + 1);
    std::string meta = std::to_string(static_cast<int>(error_type));
    auto data = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(
        meta.data())); 
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(
        data, meta.size());
    store_provider_->Put(RayObject(nullptr, meta_buffer), object_id);
  }
}

CoreWorkerDirectActorTaskReceiver::CoreWorkerDirectActorTaskReceiver(
    CoreWorkerObjectInterface &object_interface,
    boost::asio::io_service &io_service,
    rpc::GrpcServer &server, const TaskHandler &task_handler)
    : object_interface_(object_interface),
      task_service_(io_service, *this),
      task_handler_(task_handler) {
  
  server.RegisterService(task_service_);
}

void CoreWorkerDirectActorTaskReceiver::HandlePushTask(
    const rpc::PushTaskRequest &request,
    rpc::PushTaskReply *reply,
    rpc::SendReplyCallback send_reply_callback) {

  const std::string &task_message = request.task_spec();
  const TaskSpecification spec(task_message);


  if (HasByReferenceArgs(spec)) {
    send_reply_callback(Status::Invalid("direct actor call only supports by value arguments"), nullptr, nullptr);
    return;
  }

  std::vector<std::shared_ptr<Buffer>> results;
  auto status = task_handler_(spec, &results);
  RAY_CHECK(results.size() == spec.NumReturns()) << results.size() << "  " << spec.NumReturns();
  for (int i = 0; i < spec.NumReturns(); i++) {
    ObjectID id = ObjectID::ForTaskReturn(spec.TaskId(), i + 1);
    (*reply).add_return_object_ids(id.Binary());
  }

  for (int i = 0; i < results.size(); i++) {
    std::string data(reinterpret_cast<const char*>(
        const_cast<const uint8_t*>(results[i]->Data())), results[i]->Size());
    (*reply).add_return_objects(data);
  }

  send_reply_callback(status, nullptr, nullptr);
}

}  // namespace ray
