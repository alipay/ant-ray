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
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

struct VirtualCluster {
  VirtualCluster(const std::string &virtual_cluster_id) {
    proto_data_.set_id(virtual_cluster_id);
  }

  VirtualCluster(VirtualCluster &&virtual_cluster)
      : proto_data_(std::move(virtual_cluster.proto_data_)) {}

  VirtualCluster(const VirtualCluster &virtual_cluster)
      : proto_data_(virtual_cluster.proto_data_) {}

  VirtualCluster(rpc::VirtualClusterTableData data) : proto_data_(std::move(data)) {}

  VirtualCluster(const rpc::VirtualClusterTableData &data) : proto_data_(data) {}

  VirtualCluster &operator=(const VirtualCluster &nodegroup_data) = delete;

  const std::string &ID() const { return proto_data_.id(); }

  const std::string &Name() const { return proto_data_.name(); }
  void Name(const std::string &name) { proto_data_.set_name(name); }

  uint64_t Revision() const { return proto_data_.revision(); }
  void Revision(uint64_t revision) { proto_data_.set_revision(revision); }

  rpc::JobExecMode Mode() const { return proto_data_.mode(); }
  void Mode(rpc::JobExecMode mode) { proto_data_.set_mode(mode); }

  void UdpateReplicaSetList(
      google::protobuf::RepeatedPtrField<rpc::ReplicaSet> replica_set_list) {
    *proto_data_.mutable_replica_set_list() = std::move(replica_set_list);
  }

  void InsertNodeInstances(
      absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances) {
    for (auto &[id, node_instance] : node_instances) {
      (*proto_data_.mutable_node_instances())[id] = *node_instance;
    }
  }

  void RemoveNodeInstances(
      absl::flat_hash_map<std::string, const rpc::NodeInstance *> node_instances) {
    for (auto &[id, _] : node_instances) {
      proto_data_.mutable_node_instances()->erase(id);
    }
  }

  Status LookupIdleNodeInstances(
      const std::vector<rpc::ReplicaSet> &replica_set_list,
      absl::flat_hash_map<std::string, const rpc::NodeInstance *> &node_instances) const;

  const rpc::VirtualClusterTableData &Data() const { return proto_data_; }

 private:
  rpc::VirtualClusterTableData proto_data_;
};

}  // namespace gcs
}  // namespace ray