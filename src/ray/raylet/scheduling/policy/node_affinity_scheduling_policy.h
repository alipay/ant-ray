// Copyright 2021 The Ray Authors.
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

#include <vector>

#include "ray/raylet/scheduling/policy/hybrid_scheduling_policy.h"
#include "ray/raylet/scheduling/policy/scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

// Select the node based on the user specified node affinity.
// If soft is false, only the specified node might be selected.
// If soft is true and the specified node doesn't exist or is infeasible,
// hybrid policy will be used to select another node.
class NodeAffinitySchedulingPolicy : public ISchedulingPolicy {
 public:
  NodeAffinitySchedulingPolicy(
      scheduling::NodeID local_node_id,
      const absl::flat_hash_map<scheduling::NodeID, Node> &nodes,
      std::function<bool(scheduling::NodeID)> is_node_alive,
      std::function<bool(scheduling::NodeID, const SchedulingContext *)>
          is_node_schedulable)
      : local_node_id_(local_node_id),
        nodes_(nodes),
        is_node_alive_(is_node_alive),
        hybrid_policy_(local_node_id_, nodes_, is_node_alive_, is_node_schedulable),
        is_node_schedulable_(is_node_schedulable) {}

  scheduling::NodeID Schedule(const ResourceRequest &resource_request,
                              SchedulingOptions options) override;

  const scheduling::NodeID local_node_id_;
  const absl::flat_hash_map<scheduling::NodeID, Node> &nodes_;
  std::function<bool(scheduling::NodeID)> is_node_alive_;
  HybridSchedulingPolicy hybrid_policy_;
  std::function<bool(scheduling::NodeID, const SchedulingContext *)> is_node_schedulable_;
};
}  // namespace raylet_scheduling_policy
}  // namespace ray
