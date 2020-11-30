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
// limitations under the License

#include "ray/raylet/node_placement_group_manager.h"

#include <cctype>
#include <fstream>
#include <memory>

namespace ray {

namespace raylet {

NodePlacementGroupManager::NodePlacementGroupManager(ResourceIdSet &local_available_resources_,
                            std::unordered_map<NodeID, SchedulingResources> &cluster_resource_map_,
                            const NodeID &self_node_id_)
                            :local_available_resources_(local_available_resources_),
                            cluster_resource_map_(cluster_resource_map_),
                            self_node_id_(self_node_id_) {}

bool NodePlacementGroupManager::PrepareBundleResources(const BundleSpecification &bundle_spec) {
  // We will first delete the existing bundle to ensure idempotent.
  // The reason why we do this is: after GCS restarts, placement group can be rescheduled
  // directly without rolling back the operations performed before the restart.
  const auto &bundle_id = bundle_spec.BundleId();
  auto iter = bundle_state_map_.find(bundle_id);
  if (iter != bundle_state_map_.end()) {
    if (iter->second->state == CommitState::COMMITTED) {
      // If the bundle state is already committed, it means that prepare request is just
      // stale.
      RAY_LOG(INFO) << "Duplicate prepare bundle request, skip it directly.";
      return true;
    } else {
      // If there was a bundle in prepare state, it already locked resources, we will
      // return bundle resources.
      ReturnBundleResources(bundle_spec);
    }
  }

  auto &local_resource_set = cluster_resource_map_[self_node_id_];
  auto bundle_state = std::make_shared<BundleState>();
  bool local_resource_enough = bundle_spec.GetRequiredResources().IsSubset(local_resource_set.GetAvailableResources());

  if (local_resource_enough) {
    // Register states.
    auto it = bundle_state_map_.find(bundle_id);
    // Same bundle cannot be rescheduled.
    RAY_CHECK(it == bundle_state_map_.end());

    // Prepare resources. This shouldn't create formatted placement group resources
    // because that'll be done at the commit phase.
    bundle_state->acquired_resources =
        local_available_resources_.Acquire(bundle_spec.GetRequiredResources());
    local_resource_set.Acquire(bundle_spec.GetRequiredResources());

    // Register bundle state.
    bundle_state->state = CommitState::PREPARED;
    bundle_state_map_.emplace(bundle_id, bundle_state);
    bundle_spec_map_.emplace(
        bundle_id, std::make_shared<BundleSpecification>(bundle_spec.GetMessage()));
  }
  return bundle_state->acquired_resources.AvailableResources().size() > 0;
}

void NodePlacementGroupManager::CommitBundleResources(const BundleSpecification &bundle_spec) {
  // TODO(sang): It is currently not idempotent because we don't retry. Make it idempotent
  // once retry is implemented.
  const auto &bundle_id = bundle_spec.BundleId();
  auto it = bundle_state_map_.find(bundle_id);
  // When bundle is committed, it should've been prepared already.
  // If GCS call `CommitBundleResources` after `CancelResourceReserve`, we will skip it
  // directly.
  if (it == bundle_state_map_.end()) {
    RAY_LOG(INFO) << "The bundle has been cancelled. Skip it directly. Bundle info is "
                  << bundle_spec.DebugString();
    return;
  }
  const auto &bundle_state = it->second;
  bundle_state->state = CommitState::COMMITTED;
  const auto &acquired_resources = bundle_state->acquired_resources;

  const auto &bundle_resource_labels = bundle_spec.GetAllPlacementGroupResourceLabels();
  for (const auto &resource: bundle_resource_labels) {
    local_available_resources_.AddOrUpdateResource(resource.first, resource.second);
  }

  cluster_resource_map_[self_node_id_].AddOrUpdateResource(ResourceSet(bundle_resource_labels));

  RAY_CHECK(acquired_resources.AvailableResources().size() > 0)
      << "Prepare should've been failed if there were no acquireable resources.";
}

void NodePlacementGroupManager::ReturnBundleResources(const BundleSpecification &bundle_spec) {
  // We should commit resources if it weren't because
  // ReturnBundleResources requires resources to be committed when it is called.
  auto it = bundle_state_map_.find(bundle_spec.BundleId());
  if (it == bundle_state_map_.end()) {
    RAY_LOG(INFO) << "Duplicate cancel request, skip it directly.";
    return;
  }
  const auto &bundle_state = it->second;
  if (bundle_state->state == CommitState::PREPARED) {
    CommitBundleResources(bundle_spec);
  }
  bundle_state_map_.erase(it);

  // Return resources.
  const auto &resource_set = bundle_spec.GetRequiredResources();
  const auto &placement_group_resource_labels = bundle_spec.GetAllPlacementGroupResourceLabels();
  
  // Return resources to local_available_resources_.
  local_available_resources_.Release(ResourceIdSet(resource_set));
  local_available_resources_.Acquire(ResourceSet(placement_group_resource_labels));

  // Return resources to SchedulingResources.
  cluster_resource_map_[self_node_id_].Release(resource_set);
  cluster_resource_map_[self_node_id_].Acquire(ResourceSet(placement_group_resource_labels));
}

void NodePlacementGroupManager::ReturnUnusedBundleResources(const std::unordered_set<BundleID, pair_hash> &in_use_bundles) {
  for (auto iter = bundle_spec_map_.begin(); iter != bundle_spec_map_.end();) {
    if (0 == in_use_bundles.count(iter->first)) {
      ReturnBundleResources(*iter->second);
      bundle_spec_map_.erase(iter++);
    } else {
      iter++;
    }
  }
}

}  // namespace raylet

}  // namespace ray