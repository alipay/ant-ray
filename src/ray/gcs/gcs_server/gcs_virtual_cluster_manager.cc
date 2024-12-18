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

void GcsVirtualClusterManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  primary_cluster_->OnNodeAdd(node);
}

void GcsVirtualClusterManager::OnNodeDead(const rpc::GcsNodeInfo &node) {
  primary_cluster_->OnNodeDead(node);
}

std::shared_ptr<VirtualCluster> GcsVirtualClusterManager::GetVirtualCluster(
    const std::string &virtual_cluster_id) {
  // Check if it is a logical cluster
  auto logical_cluster = primary_cluster_->GetLogicalCluster(virtual_cluster_id);
  if (logical_cluster != nullptr) {
    return logical_cluster;
  }
  // Check if it is a job cluster
  auto job_cluster = primary_cluster_->GetJobCluster(virtual_cluster_id);
  if (job_cluster != nullptr) {
    return job_cluster;
  }
  // Check if it is a job cluster of any logical cluster
  auto logical_clusters = primary_cluster_->GetAllLogicalClusters();
  for (auto &[cluster_id, logical_cluster] : logical_clusters) {
    ExclusiveCluster *exclusive_cluster =
        dynamic_cast<ExclusiveCluster *>(logical_cluster.get());
    auto job_cluster = exclusive_cluster->GetJobCluster(virtual_cluster_id);
    if (job_cluster != nullptr) {
      return job_cluster;
    }
  }
  return nullptr;
}

void GcsVirtualClusterManager::HandleCreateOrUpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    rpc::CreateOrUpdateVirtualClusterReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  RAY_LOG(INFO) << "Start creating or updating virtual cluster " << virtual_cluster_id;
  auto on_done = [reply, virtual_cluster_id, callback = std::move(send_reply_callback)](
                     const Status &status,
                     std::shared_ptr<rpc::VirtualClusterTableData> data) {
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

  // Verify if the arguments in the request is valid.
  auto status = VerifyRequest(request);
  if (status.ok()) {
    status = primary_cluster_->CreateOrUpdateVirtualCluster(std::move(request),
                                                            std::move(on_done));
  }
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
  auto on_done = [reply, virtual_cluster_id, callback = std::move(send_reply_callback)](
                     const Status &status,
                     std::shared_ptr<rpc::VirtualClusterTableData> data) {
    if (status.ok()) {
      RAY_LOG(INFO) << "Succeed in removing virtual cluster " << virtual_cluster_id;
    } else {
      RAY_LOG(ERROR) << "Failed to remove virtual cluster " << virtual_cluster_id
                     << ", status = " << status.ToString();
    }
    GCS_RPC_SEND_REPLY(callback, reply, status);
  };

  auto status = VerifyRequest(request);
  if (!status.ok()) {
    on_done(status, nullptr);
    return;
  }

  status = primary_cluster_->RemoveLogicalCluster(virtual_cluster_id, on_done);
  if (!status.ok()) {
    on_done(status, nullptr);
  }
}

void GcsVirtualClusterManager::HandleGetAllVirtualClusters(
    rpc::GetAllVirtualClustersRequest request,
    rpc::GetAllVirtualClustersReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all virtual clusters.";
  // TODO(Shanly): To be implement.
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
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

  if (virtual_cluster_id == primary_cluster_->GetID()) {
    std::ostringstream ostr;
    ostr << "Invalid request, " << virtual_cluster_id
         << " can not be created or updated.";
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }

  for (const auto &[template_id, replicas] : request.replica_sets()) {
    if (replicas < 0) {
      std::ostringstream ostr;
      ostr << "Invalid request, replicas(" << replicas
           << ") must >= 0, virtual_cluster_id: " << virtual_cluster_id;
      auto message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }

    if (template_id.empty()) {
      std::ostringstream ostr;
      ostr << "Invalid request, template_id is empty, virtual_cluster_id: "
           << virtual_cluster_id;
      auto message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }
  }

  if (auto logical_cluster =
          primary_cluster_->GetLogicalCluster(request.virtual_cluster_id())) {
    // Check if the revision of the virtual cluster is expired.
    if (request.revision() != logical_cluster->GetRevision()) {
      std::ostringstream ss;
      ss << "The revision (" << request.revision()
         << ") is expired, the latest revision of the virtual cluster "
         << request.virtual_cluster_id() << " is " << logical_cluster->GetRevision();
      std::string message = ss.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }

    // check if the request attributes are compatible with the virtual cluster.
    if (request.mode() != logical_cluster->GetMode()) {
      std::ostringstream ostr;
      ostr << "The requested attributes are incompatible with virtual cluster "
           << request.virtual_cluster_id() << ". expect: (" << logical_cluster->GetMode()
           << "), actual: (" << request.mode() << ").";
      std::string message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::InvalidArgument(message);
    }
  }

  return Status::OK();
}

Status GcsVirtualClusterManager::VerifyRequest(
    const rpc::RemoveVirtualClusterRequest &request) {
  const auto &virtual_cluster_id = request.virtual_cluster_id();
  if (virtual_cluster_id.empty()) {
    std::ostringstream ostr;
    ostr << "Invalid request, the virtual cluster id is empty.";
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }

  if (virtual_cluster_id == primary_cluster_->GetID()) {
    std::ostringstream ostr;
    ostr << "Invalid request, " << virtual_cluster_id << " can not be removed.";
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::InvalidArgument(message);
  }
  return Status::OK();
}

Status GcsVirtualClusterManager::FlushAndPublish(
    std::shared_ptr<rpc::VirtualClusterTableData> data,
    CreateOrUpdateVirtualClusterCallback callback) {
  auto on_done = [this, data, callback = std::move(callback)](const Status &status) {
    // The backend storage is supposed to be reliable, so the status must be ok.
    RAY_CHECK_OK(status);
    RAY_CHECK_OK(gcs_publisher_.PublishVirtualCluster(
        VirtualClusterID::FromBinary(data->id()), *data, nullptr));
    if (callback) {
      callback(status, std::move(data));
    }
  };

  if (data->is_removed()) {
    return gcs_table_storage_.VirtualClusterTable().Delete(
        VirtualClusterID::FromBinary(data->id()), on_done);
  }

  // Write the virtual cluster data to the storage.
  return gcs_table_storage_.VirtualClusterTable().Put(
      VirtualClusterID::FromBinary(data->id()), *data, on_done);
}
}  // namespace gcs
}  // namespace ray