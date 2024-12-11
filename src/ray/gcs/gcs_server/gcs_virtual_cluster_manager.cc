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

#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

#include "gcs_virtual_cluster_manager.h"

namespace ray {
namespace gcs {

void GcsVirtualClusterManager::Initialize(const GcsInitData &gcs_init_data) {
  // TODO(Shanly): To be implement.
}

void GcsVirtualClusterManager::HandleCreateOrUpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    rpc::CreateOrUpdateVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  RAY_LOG(INFO) << "Start creating or updating virtual cluster " << virtual_cluster_id;
  auto on_done = [reply, virtual_cluster_id, callback = std::move(send_reply_callback)](
                     const Status &status, const rpc::VirtualClusterTableData *data) {
    if (status.ok()) {
      RAY_CHECK(data != nullptr);
      // Fill the node instances of the virtual cluster to the reply.
      reply->mutable_node_instances()->insert(data->node_instances().begin(),
                                              data->node_instances().end());
      // Fill the revision of the virtual cluster to the reply.
      reply->set_revision(data->revision());
      RAY_LOG(INFO) << "Succeed in creating or updating virtual cluster " << data->id();
    } else {
      RAY_CHECK(data == nullptr);
      RAY_LOG(WARNING) << "Failed to create or update virtual cluster "
                       << virtual_cluster_id << ", status = " << status.ToString();
    }
    GCS_RPC_SEND_REPLY(callback, reply, status);
  };
  auto status = CreateOrUpdateVirtualCluster(std::move(request), on_done);
  if (!status.ok()) {
    on_done(status, nullptr);
  }
}

void GcsVirtualClusterManager::HandleRemoveVirtualCluster(
    rpc::RemoveVirtualClusterRequest request,
    rpc::RemoveVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  RAY_LOG(INFO) << "Start removing virtual cluster " << virtual_cluster_id;
  // TODO(Shanly): To be implement.
}

void GcsVirtualClusterManager::HandleGetAllVirtualClusters(
    rpc::GetAllVirtualClustersRequest request,
    rpc::GetAllVirtualClustersReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all virtual clusters.";
  // TODO(Shanly): To be implement.
}

std::shared_ptr<VirtualCluster> GcsVirtualClusterManager::GetVirtualCluster(
    const std::string &virtual_cluster_id) const {
  auto iter = virtual_clusters_.find(virtual_cluster_id);
  return iter != virtual_clusters_.end() ? iter->second : nullptr;
}

Status GcsVirtualClusterManager::VerifyRequest(
    const rpc::CreateOrUpdateVirtualClusterRequest &request) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  if (virtual_cluster_id.empty()) {
    std::ostringstream ostr;
    ostr << "Invalid request, the virtual cluster id is empty.";
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }

  if (virtual_cluster_id == kDefaultVirtualClusterID) {
    std::ostringstream ostr;
    ostr << "Invalid request, " << virtual_cluster_id
         << " is a system reserved virtual cluster id.";
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }

  for (const auto &replica_set : request.replica_set_list()) {
    auto replicas = replica_set.replicas();
    if (replicas < 0) {
      std::ostringstream ostr;
      ostr << "Invalid request, replicas(" << replicas
           << ") must >= 0, virtual_cluster_id: " << virtual_cluster_id;
      auto message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }

    const auto &template_id = replica_set.template_id();
    if (template_id.empty()) {
      std::ostringstream ostr;
      ostr << "Invalid request, template_id is empty, virtual_cluster_id: "
           << virtual_cluster_id;
      auto message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }
  }

  if (auto virtual_cluster = GetVirtualCluster(request.virtual_cluster_id())) {
    // Check if the revision of the virtual cluster is expired.
    if (request.revision() != virtual_cluster->Revision()) {
      std::ostringstream ss;
      ss << "The revision (" << request.revision()
         << ") is expired, the latest revision of the virtual cluster "
         << request.virtual_cluster_id() << " is " << virtual_cluster->Revision();
      std::string message = ss.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }

    // check if the request attributes are compatible with the virtual cluster.
    if (request.mode() != virtual_cluster->Mode()) {
      std::ostringstream ostr;
      ostr << "The requested attributes are incompatible with virtual cluster "
           << request.virtual_cluster_id() << ". expect: (" << virtual_cluster->Mode()
           << "), actual: (" << request.mode() << ").";
      std::string message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }
  }

  return Status::OK();
}

Status GcsVirtualClusterManager::CreateOrUpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    CreateOrUpdateVirtualClusterCallback callback) {
  // Verify if the arguments in the request is valid.
  auto status = VerifyRequest(request);
  if (!status.ok()) {
    return status;
  }

  // Calculate the node instances that to be added and to be removed.
  absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances_to_add;
  absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances_to_remove;
  status = DetermineNodeInstanceAdditionsAndRemovals(
      request, &node_instances_to_add, &node_instances_to_remove);
  if (!status.ok()) {
    return status;
  }

  const auto &virtual_cluster_id = request.virtual_cluster_id();
  if (auto cached_virtual_cluster = GetVirtualCluster(virtual_cluster_id)) {
    // Update virtual cluster.
    return UpdateVirtualCluster(std::move(request),
                                std::move(node_instances_to_add),
                                std::move(node_instances_to_remove),
                                std::move(callback));
  }

  // node_instances_to_remove must be empty as the virtual cluster is a new one.
  RAY_CHECK(node_instances_to_remove.empty());
  return AddVirtualCluster(
      std::move(request), std::move(node_instances_to_add), std::move(callback));
}

std::vector<rpc::ReplicaSet> ReplicasDifference(
    const google::protobuf::RepeatedPtrField<rpc::ReplicaSet> &left,
    const google::protobuf::RepeatedPtrField<rpc::ReplicaSet> &right) {
  std::vector<rpc::ReplicaSet> result;
  for (auto iter = left.begin(); iter != left.end(); ++iter) {
    auto replica_set = *iter;
    auto iter_right = std::find_if(
        right.begin(),
        right.end(),
        [&replica_set](const rpc::ReplicaSet &right_replica_set) {
          return right_replica_set.template_id() == replica_set.template_id();
        });
    if (iter_right == right.end()) {
      result.emplace_back(std::move(replica_set));
    } else {
      if (iter_right->replicas() < replica_set.replicas()) {
        replica_set.set_replicas(replica_set.replicas() - iter_right->replicas());
        result.emplace_back(std::move(replica_set));
      }
    }
  }
  return result;
}

Status GcsVirtualClusterManager::DetermineNodeInstanceAdditionsAndRemovals(
    const rpc::CreateOrUpdateVirtualClusterRequest &request,
    absl::flat_hash_map<std::string, const rpc::NodeInstance *> *node_instances_to_add,
    absl::flat_hash_map<std::string, const rpc::NodeInstance *>
        *node_instances_to_remove) {
  RAY_CHECK(node_instances_to_add != nullptr && node_instances_to_remove != nullptr);
  node_instances_to_add->clear();
  node_instances_to_remove->clear();

  const auto &virtual_cluster_id = request.virtual_cluster_id();
  auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
  if (virtual_cluster != nullptr) {
    auto replicas_to_remove = ReplicasDifference(
        virtual_cluster->Data().replica_set_list(), request.replica_set_list());
    // Lookup idle node instances from the virtual cluster based on `replicas_to_remove`.
    auto status = virtual_cluster->LookupIdleNodeInstances(replicas_to_remove,
                                                           *node_instances_to_remove);
    if (!status.ok()) {
      return status;
    }
  }

  auto replicas_to_add = ReplicasDifference(
      request.replica_set_list(),
      virtual_cluster ? virtual_cluster->Data().replica_set_list()
                      : google::protobuf::RepeatedPtrField<rpc::ReplicaSet>());
  auto virtual_cluster_owner_id = VirtualClusterID::OwnerID(virtual_cluster_id);
  auto owner_virtual_cluster = GetVirtualCluster(virtual_cluster_owner_id.Binary());
  RAY_CHECK(owner_virtual_cluster != nullptr);
  // Lookup idle node instances from the owner virtual cluster based on
  // `replicas_to_add`.
  return owner_virtual_cluster->LookupIdleNodeInstances(replicas_to_add,
                                                        *node_instances_to_add);
}

Status GcsVirtualClusterManager::AddVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances_to_add,
    CreateOrUpdateVirtualClusterCallback callback) {
  auto local_virtual_cluster = GetVirtualCluster(request.virtual_cluster_id());
  RAY_CHECK(local_virtual_cluster == nullptr);

  auto virtual_cluster = std::make_shared<VirtualCluster>(request.virtual_cluster_id());
  virtual_cluster->Name(request.virtual_cluster_name());
  virtual_cluster->Mode(request.mode());
  // Update the replica set list of the virtual cluster.
  virtual_cluster->UdpateReplicaSetList(std::move(*request.mutable_replica_set_list()));
  // Insert node instances to the virtual cluster.
  virtual_cluster->InsertNodeInstances(std::move(node_instances_to_add));
  // Update the revision of the virtual cluster.
  virtual_cluster->Revision(current_sys_time_ns());

  // Insert virtual cluster to the map.
  RAY_CHECK(virtual_clusters_.emplace(virtual_cluster->ID(), virtual_cluster).second);

  const auto &virtual_cluster_data = virtual_cluster->Data();
  auto on_done = [this, virtual_cluster_data, callback](const Status &status) {
    // The backend storage is supposed to be reliable, so the status must be ok.
    RAY_CHECK_OK(status);
    RAY_CHECK_OK(gcs_publisher_.PublishVirtualCluster(
        VirtualClusterID::FromBinary(virtual_cluster_data.id()),
        virtual_cluster_data,
        nullptr));
    if (callback) {
      callback(status, &virtual_cluster_data);
    }
  };

  // Write the virtual cluster data to the storage.
  return gcs_table_storage_.VirtualClusterTable().Put(
      VirtualClusterID::FromBinary(virtual_cluster->ID()), virtual_cluster_data, on_done);
}

Status GcsVirtualClusterManager::UpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances_to_add,
    absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances_to_remove,
    CreateOrUpdateVirtualClusterCallback callback,
    bool force_remove_nodes_if_needed /*=false*/) {
  auto virtual_cluster = GetVirtualCluster(request.virtual_cluster_id());
  RAY_CHECK(virtual_cluster != nullptr)
      << " virtual cluster " << request.virtual_cluster_id() << " does not exist.";

  // virtual_cluster->Name(request.virtual_cluster_name());
  // virtual_cluster->Mode(request.mode());
  // Update the replica set list of the virtual cluster.
  virtual_cluster->UdpateReplicaSetList(std::move(*request.mutable_replica_set_list()));
  // Insert node instances to the virtual cluster.
  virtual_cluster->InsertNodeInstances(std::move(node_instances_to_add));
  // Remove node instances from the virtual cluster.
  virtual_cluster->RemoveNodeInstances(std::move(node_instances_to_remove));
  // Update the revision of the virtual cluster.
  virtual_cluster->Revision(current_sys_time_ns());

  const auto &virtual_cluster_data = virtual_cluster->Data();
  auto on_done = [this, virtual_cluster_data, callback](const Status &status) {
    // The backend storage is supposed to be reliable, so the status must be ok.
    RAY_CHECK_OK(status);
    RAY_CHECK_OK(gcs_publisher_.PublishVirtualCluster(
        VirtualClusterID::FromBinary(virtual_cluster_data.id()),
        virtual_cluster_data,
        nullptr));
    if (callback) {
      callback(status, &virtual_cluster_data);
    }
  };

  // Write the virtual cluster data to the storage.
  return gcs_table_storage_.VirtualClusterTable().Put(
      VirtualClusterID::FromBinary(virtual_cluster->ID()), virtual_cluster_data, on_done);
}

}  // namespace gcs
}  // namespace ray