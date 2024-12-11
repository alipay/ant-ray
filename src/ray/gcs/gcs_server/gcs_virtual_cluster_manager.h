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

#pragma once

#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_server/gcs_virtual_cluster.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// This implementation class of `VirtualClusterInfoHandler`.
class GcsVirtualClusterManager : public rpc::VirtualClusterInfoHandler {
  using CreateOrUpdateVirtualClusterCallback =
      std::function<void(const Status &, const rpc::VirtualClusterTableData *)>;

 public:
  explicit GcsVirtualClusterManager(GcsTableStorage &gcs_table_storage,
                                    GcsPublisher &gcs_publisher)
      : gcs_table_storage_(gcs_table_storage), gcs_publisher_(gcs_publisher) {}

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

 protected:
  void HandleCreateOrUpdateVirtualCluster(
      rpc::CreateOrUpdateVirtualClusterRequest request,
      rpc::CreateOrUpdateVirtualClusterReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveVirtualCluster(rpc::RemoveVirtualClusterRequest request,
                                  rpc::RemoveVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllVirtualClusters(rpc::GetAllVirtualClustersRequest request,
                                   rpc::GetAllVirtualClustersReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) override;

  std::shared_ptr<VirtualCluster> GetVirtualCluster(
      const std::string &virtual_cluster_id) const;

  Status CreateOrUpdateVirtualCluster(rpc::CreateOrUpdateVirtualClusterRequest request,
                                      CreateOrUpdateVirtualClusterCallback callback);

  Status VerifyRequest(const rpc::CreateOrUpdateVirtualClusterRequest &request);

  /// Add a new virtual cluster.
  ///
  /// \param virtual_cluster The virtual cluster to add.
  /// \param callback The callback that will be called after the virtual cluster has been
  /// added.
  /// \return Status.
  Status AddVirtualCluster(
      rpc::CreateOrUpdateVirtualClusterRequest request,
      absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances_to_add,
      CreateOrUpdateVirtualClusterCallback callback);

  Status UpdateVirtualCluster(
      rpc::CreateOrUpdateVirtualClusterRequest request,
      absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances_to_add,
      absl::flat_hash_map<std::string, const rpc::NodeInstance *>
          node_instances_to_remove,
      CreateOrUpdateVirtualClusterCallback callback,
      bool force_remove_nodes_if_needed = false);

  /// Calculate the node instances that to be added and to be removed
  /// based on the demand final replica sets inside the request.
  ///
  /// \param request The request to create or update virtual cluster.
  /// \param node_instances_to_add The node instances that to be added.
  /// \param node_instances_to_remove The node instances that to be removed.
  /// \return status The status of the calculation.
  Status DetermineNodeInstanceAdditionsAndRemovals(
      const rpc::CreateOrUpdateVirtualClusterRequest &request,
      absl::flat_hash_map<std::string, const rpc::NodeInstance *> *node_instances_to_add,
      absl::flat_hash_map<std::string, const rpc::NodeInstance *>
          *node_instances_to_remove);

 private:
  /// The storage of the GCS tables.
  GcsTableStorage &gcs_table_storage_;
  /// The publisher of the GCS tables.
  GcsPublisher &gcs_publisher_;

  /// The map of virtual clusters.
  /// Mapping from virtual cluster id to the virtual cluster.
  absl::flat_hash_map<std::string, std::shared_ptr<VirtualCluster>> virtual_clusters_;
};

}  // namespace gcs
}  // namespace ray