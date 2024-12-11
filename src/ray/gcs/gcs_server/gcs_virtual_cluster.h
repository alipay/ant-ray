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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/status.h"
#include "ray/common/virtual_cluster_id.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

struct NodeInstance {
  NodeInstance() = default;

  const std::string &hostname() const { return hostname_; }
  void set_hostname(const std::string &hostname) { hostname_ = hostname; }

  const std::string &template_id() const { return template_id_; }
  void set_template_id(const std::string &template_id) { template_id_ = template_id; }

  bool is_dead() const { return is_dead_; }
  void set_is_dead(bool is_dead) { is_dead_ = is_dead; }

  std::shared_ptr<rpc::NodeInstance> ToProto() const {
    auto node_instance = std::make_shared<rpc::NodeInstance>();
    node_instance->set_template_id(template_id_);
    node_instance->set_hostname(hostname_);
    return node_instance;
  }

 private:
  std::string hostname_;
  std::string template_id_;
  bool is_dead_ = false;
};

static const std::string kEmptyJobClusterId = "";
using CreateOrUpdateVirtualClusterCallback =
    std::function<void(const Status &, std::shared_ptr<rpc::VirtualClusterTableData>)>;

/// <template_id, _>
///               |
///               |--> <job_cluster_id, _>
///                                     |
///                                     |--> <node_instance_id,
///                                     std::shared_ptr<gcs::NodeInstance>>
using ReplicaInstances = absl::flat_hash_map<
    std::string,
    absl::flat_hash_map<
        std::string,
        absl::flat_hash_map<std::string, std::shared_ptr<gcs::NodeInstance>>>>;

using ReplicaSets = absl::flat_hash_map<std::string, int32_t>;

using AsyncClusterDataFlusher = std::function<Status(
    std::shared_ptr<rpc::VirtualClusterTableData>, CreateOrUpdateVirtualClusterCallback)>;

/// Calculate the difference between two replica sets.
template <typename T1, typename T2>
ReplicaSets ReplicasDifference(const T1 &left, const T2 &right) {
  ReplicaSets result;
  for (const auto &[template_id, replicas] : left) {
    auto right_iter = right.find(template_id);
    if (right_iter == right.end()) {
      result[template_id] = replicas;
    } else {
      if (right_iter->second < replicas) {
        result[template_id] = replicas - right_iter->second;
      }
    }
  }
  return result;
}

class JobCluster;
class AbstractCluster {
 public:
  virtual ~AbstractCluster() = default;

  /// Get the id of the cluster.
  virtual const std::string &GetID() const = 0;

  /// Get the id of the cluster.
  virtual const std::string &GetName() const = 0;

  /// Get the workerload mode of the cluster.
  /// There are two modes of the cluster:
  ///  - Exclusive mode means that a signle node in the cluster can execute one or
  ///  more tasks belongs to only one job.
  ///  - Mixed mode means that a single node in the cluster can execute tasks
  ///  belongs to multiple jobs.
  /// \return The workload mode of the cluster.
  virtual rpc::WorkloadMode GetMode() const = 0;

  /// Get the revision number of the cluster.
  uint64_t GetRevision() const { return revision_; }

  /// Get the replica sets corresponding to the cluster.
  const ReplicaSets &GetReplicaSets() const { return replica_sets_; }

  /// Get the visible node instances of the cluster.
  const ReplicaInstances &GetVisibleNodeInstances() const {
    return visible_node_instances_;
  }

  /// Update the node instances of the cluster.
  ///
  /// \param replica_instances_to_add The node instances to be added.
  /// \param replica_instances_to_remove The node instances to be removed.
  void UpdateNodeInstances(ReplicaInstances replica_instances_to_add,
                           ReplicaInstances replica_instances_to_remove);

  /// Lookup idle node instances from `visible_node_instances_` based on the demand final
  /// replica sets.
  ///
  /// \param replica_sets The demand final replica sets.
  /// \param replica_instances The node instances lookuped best effort from the visible
  /// node instances.
  /// \return OK if the lookup is successful, otherwise return an error.
  Status LookupIdleNodeInstances(const ReplicaSets &replica_sets,
                                 ReplicaInstances &replica_instances) const;

  /// Mark the node instance as dead.
  ///
  /// \param node_instance_id The id of the node instance to be marked as dead.
  /// \return True if the node instance is marked as dead, false otherwise.
  bool MarkNodeInstanceAsDead(const std::string &template_id,
                              const std::string &node_instance_id);

  /// Convert the virtual cluster to proto data which usually is used for flushing
  /// to redis or publishing to raylet.
  /// \return A shared pointer to the proto data.
  std::shared_ptr<rpc::VirtualClusterTableData> ToProto() const;

 protected:
  /// Check if the node instance is idle. If a node instance is idle, it can be
  /// removed from the virtual cluster safely.
  /// \param node_instance The node instance to be checked.
  /// \return True if the node instance is idle, false otherwise.
  virtual bool IsIdleNodeInstance(const std::string &job_cluster_id,
                                  const gcs::NodeInstance &node_instance) const = 0;

  /// Insert the node instances to the cluster.
  ///
  /// \param replica_instances The node instances to be inserted.
  void InsertNodeInstances(ReplicaInstances replica_instances);

  /// Remove the node instances from the cluster.
  ///
  /// \param replica_instances The node instances to be removed.
  void RemoveNodeInstances(ReplicaInstances replica_instances);

  /// Node instances that are visible to the cluster.
  ReplicaInstances visible_node_instances_;
  /// Replica sets to express the visible node instances.
  ReplicaSets replica_sets_;
  // Version number of the last modification to the cluster.
  uint64_t revision_{0};
};

class JobClusterManager : public AbstractCluster {
 public:
  JobClusterManager(const AsyncClusterDataFlusher &async_data_flusher)
      : async_data_flusher_(async_data_flusher) {}

  /// Create a job cluster.
  ///
  /// \param job_id The job ID associated with the job cluster to crate.
  /// \param replica_sets The replica sets of the job cluster.
  /// \return Status The status of the creation.
  Status CreateJobCluster(const std::string &job_id,
                          const std::string &cluster_id,
                          ReplicaSets replica_sets,
                          CreateOrUpdateVirtualClusterCallback callback);

 protected:
  // The mapping from job cluster id to `JobCluster` instance.
  absl::flat_hash_map<std::string, std::shared_ptr<JobCluster>> job_clusters_;
  // The async data flusher.
  AsyncClusterDataFlusher async_data_flusher_;
};

class VirtualCluster;
class PrimaryCluster : public JobClusterManager {
 public:
  PrimaryCluster(const AsyncClusterDataFlusher &async_data_flusher)
      : JobClusterManager(async_data_flusher) {}

  const std::string &GetID() const override { return kDefaultVirtualClusterID; }
  rpc::WorkloadMode GetMode() const override { return rpc::WorkloadMode::Exclusive; }
  const std::string &GetName() const override { return kDefaultVirtualClusterID; }

  /// Create or update a new virtual cluster.
  ///
  /// \param request The request to create or update a virtual cluster.
  /// cluster. \param callback The callback that will be called after the virtual cluster
  /// is flushed.
  /// \return Status.
  Status CreateOrUpdateVirtualCluster(rpc::CreateOrUpdateVirtualClusterRequest request,
                                      CreateOrUpdateVirtualClusterCallback callback);

  /// Get the virtual cluster by the virtual cluster id.
  ///
  /// \param virtual_cluster_id The id of the virtual cluster.
  /// \return The virtual cluster if it exists, otherwise return nullptr.
  std::shared_ptr<VirtualCluster> GetVirtualCluster(
      const std::string &virtual_cluster_id) const;

  void OnNodeAdded(const rpc::GcsNodeInfo &node);

  void OnNodeRemoved(const rpc::GcsNodeInfo &node);

 protected:
  bool IsIdleNodeInstance(const std::string &job_cluster_id,
                          const gcs::NodeInstance &node_instance) const override;

 private:
  /// Calculate the node instances that to be added and to be removed
  /// based on the demand final replica sets inside the request.
  ///
  /// \param request The request to create or update virtual cluster.
  /// \param node_instances_to_add The node instances that to be added.
  /// \param node_instances_to_remove The node instances that to be removed.
  /// \return status The status of the calculation.
  Status DetermineNodeInstanceAdditionsAndRemovals(
      const rpc::CreateOrUpdateVirtualClusterRequest &request,
      ReplicaInstances &node_instances_to_add,
      ReplicaInstances &node_instances_to_remove);

  /// The map of virtual clusters.
  /// Mapping from virtual cluster id to the virtual cluster.
  absl::flat_hash_map<std::string, std::shared_ptr<VirtualCluster>> virtual_clusters_;
};

class VirtualCluster : public JobClusterManager {
 public:
  VirtualCluster(const AsyncClusterDataFlusher &async_data_flusher,
                 const std::string &id,
                 const std::string &name,
                 rpc::WorkloadMode mode)
      : JobClusterManager(async_data_flusher), id_(id), name_(name), mode_(mode) {}

  VirtualCluster &operator=(const VirtualCluster &) = delete;

  const std::string &GetID() const override { return id_; }
  rpc::WorkloadMode GetMode() const override { return mode_; }
  const std::string &GetName() const override { return name_; }

 protected:
  bool IsIdleNodeInstance(const std::string &job_cluster_id,
                          const gcs::NodeInstance &node_instance) const override;

 private:
  /// The id of the virtual cluster.
  std::string id_;
  /// The name of the virtual cluster.
  std::string name_;
  /// The workerload mode of the virtual cluster.
  rpc::WorkloadMode mode_;
};

class JobCluster : public AbstractCluster {
 public:
  JobCluster(const std::string &id, const std::string &name) : id_(id), name_(name) {}

  const std::string &GetID() const override { return id_; }
  rpc::WorkloadMode GetMode() const override { return rpc::WorkloadMode::Exclusive; }
  const std::string &GetName() const override { return id_; }

  bool IsIdleNodeInstance(const std::string &job_cluster_id,
                          const gcs::NodeInstance &node_instance) const override;

 private:
  /// The id of the job cluster.
  std::string id_;
  /// The name of the job cluster.
  std::string name_;
};

}  // namespace gcs
}  // namespace ray