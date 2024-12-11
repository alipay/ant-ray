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

#include "ray/gcs/gcs_server/gcs_virtual_cluster.h"

namespace ray {
namespace gcs {
Status VirtualCluster::LookupIdleNodeInstances(
    const std::vector<rpc::ReplicaSet> &replica_set_list,
    absl::flat_hash_map<std::string, const rpc::NodeInstance *> &node_instances) const {
  // TODO(Shanly): To be implement.
  return Status::OK();
}
}  // namespace gcs
}  // namespace ray