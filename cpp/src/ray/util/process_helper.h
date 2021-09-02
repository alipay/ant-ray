// Copyright 2020-2021 The Ray Authors.
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
#include <string>

#include "../config_internal.h"
#include "ray/core_worker/core_worker.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"

namespace ray {
namespace internal {

using ray::core::CoreWorkerOptions;

class ProcessHelper {
 public:
  void RayStart(CoreWorkerOptions::TaskExecutionCallback callback);
  void RayStop();
  void StartRayNode(const int redis_port, const std::string redis_password,
                    const int num_cpus = -1, const int num_gpus = -1,
                    const std::unordered_map<std::string, int> resources = {});
  void StopRayNode();

  static ProcessHelper &GetInstance() {
    static ProcessHelper processHelper;
    return processHelper;
  }

  std::unique_ptr<ray::gcs::GlobalStateAccessor> CreateGlobalStateAccessor(
      const std::string &redis_address, const std::string &redis_password);

  ProcessHelper(ProcessHelper const &) = delete;
  void operator=(ProcessHelper const &) = delete;

 private:
  ProcessHelper(){};
};

std::string GetNodeIpAddress(const std::string &address = "8.8.8.8:53");
}  // namespace internal
}  // namespace ray
