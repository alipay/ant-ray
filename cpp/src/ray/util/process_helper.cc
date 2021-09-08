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

#include <boost/algorithm/string.hpp>

#include "process_helper.h"
#include "ray/util/process.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace internal {

using ray::core::CoreWorkerProcess;
using ray::core::WorkerType;

std::string FormatResourcesArg(const std::unordered_map<std::string, int> &resources) {
  std::ostringstream oss;
  oss << "{";
  for (auto iter = resources.begin(); iter != resources.end();) {
    oss << "\"" << iter->first << "\":" << iter->second;
    ++iter;
    if (iter != resources.end()) {
      oss << ",";
    }
  }
  oss << "}";
  return oss.str();
}

void ProcessHelper::StartRayNode(const int redis_port, const std::string redis_password,
                                 const int num_cpus, const int num_gpus,
                                 const std::unordered_map<std::string, int> resources) {
  std::vector<std::string> cmdargs({"ray", "start", "--head", "--port",
                                    std::to_string(redis_port), "--redis-password",
                                    redis_password, "--include-dashboard", "false"});
  if (num_cpus >= 0) {
    cmdargs.emplace_back("--num-cpus");
    cmdargs.emplace_back(std::to_string(num_cpus));
  }
  if (num_gpus >= 0) {
    cmdargs.emplace_back("--num-gpus");
    cmdargs.emplace_back(std::to_string(num_gpus));
  }
  if (!resources.empty()) {
    cmdargs.emplace_back("--resources");
    cmdargs.emplace_back(FormatResourcesArg(resources));
  }
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true).second);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return;
}

void ProcessHelper::StopRayNode() {
  std::vector<std::string> cmdargs({"ray", "stop"});
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true).second);
  std::this_thread::sleep_for(std::chrono::seconds(3));
  return;
}

std::unique_ptr<ray::gcs::GlobalStateAccessor> ProcessHelper::CreateGlobalStateAccessor(
    const std::string &redis_address, const std::string &redis_password) {
  std::unique_ptr<ray::gcs::GlobalStateAccessor> global_state_accessor = nullptr;
  global_state_accessor.reset(
      new ray::gcs::GlobalStateAccessor(redis_address, redis_password));
  RAY_CHECK(global_state_accessor->Connect()) << "Failed to connect to GCS.";
  return global_state_accessor;
}

void ProcessHelper::RayStart(CoreWorkerOptions::TaskExecutionCallback callback) {
  std::string redis_ip = ConfigInternal::Instance().redis_ip;
  if (ConfigInternal::Instance().worker_type == WorkerType::DRIVER && redis_ip.empty()) {
    redis_ip = "127.0.0.1";
    StartRayNode(ConfigInternal::Instance().redis_port,
                 ConfigInternal::Instance().redis_password,
                 ConfigInternal::Instance().num_cpus, ConfigInternal::Instance().num_gpus,
                 ConfigInternal::Instance().resources);
  }
  if (redis_ip == "127.0.0.1") {
    redis_ip = GetNodeIpAddress();
  }

  std::string redis_address =
      redis_ip + ":" + std::to_string(ConfigInternal::Instance().redis_port);
  std::string node_ip = ConfigInternal::Instance().node_ip_address;
  if (node_ip.empty()) {
    if (!ConfigInternal::Instance().redis_ip.empty()) {
      node_ip = GetNodeIpAddress(redis_address);
    } else {
      node_ip = GetNodeIpAddress();
    }
  }

  std::unique_ptr<ray::gcs::GlobalStateAccessor> global_state_accessor = nullptr;
  if (ConfigInternal::Instance().worker_type == WorkerType::DRIVER) {
    global_state_accessor = CreateGlobalStateAccessor(
        redis_address, ConfigInternal::Instance().redis_password);
    std::string node_to_connect;
    auto status =
        global_state_accessor->GetNodeToConnectForDriver(node_ip, &node_to_connect);
    RAY_CHECK_OK(status);
    ray::rpc::GcsNodeInfo node_info;
    node_info.ParseFromString(node_to_connect);
    ConfigInternal::Instance().raylet_socket_name = node_info.raylet_socket_name();
    ConfigInternal::Instance().plasma_store_socket_name =
        node_info.object_store_socket_name();
    ConfigInternal::Instance().node_manager_port = node_info.node_manager_port();
  }
  RAY_CHECK(!ConfigInternal::Instance().raylet_socket_name.empty());
  RAY_CHECK(!ConfigInternal::Instance().plasma_store_socket_name.empty());
  RAY_CHECK(ConfigInternal::Instance().node_manager_port > 0);

  std::string log_dir = ConfigInternal::Instance().logs_dir;
  if (log_dir.empty()) {
    std::string session_dir = ConfigInternal::Instance().session_dir;
    if (session_dir.empty()) {
      if (!global_state_accessor) {
        global_state_accessor = std::make_unique<ray::gcs::GlobalStateAccessor>(
            redis_address, ConfigInternal::Instance().redis_password);
        RAY_CHECK(global_state_accessor->Connect()) << "Failed to connect to GCS.";
      }
      session_dir = *global_state_accessor->GetInternalKV("session_dir");
    }
    log_dir = session_dir + "/logs";
  }

  gcs::GcsClientOptions gcs_options =
      gcs::GcsClientOptions(redis_ip, ConfigInternal::Instance().redis_port,
                            ConfigInternal::Instance().redis_password);

  CoreWorkerOptions options;
  options.worker_type = ConfigInternal::Instance().worker_type;
  options.language = Language::CPP;
  options.store_socket = ConfigInternal::Instance().plasma_store_socket_name;
  options.raylet_socket = ConfigInternal::Instance().raylet_socket_name;
  if (options.worker_type == WorkerType::DRIVER) {
    if (!ConfigInternal::Instance().job_id.empty()) {
      options.job_id = JobID::FromHex(ConfigInternal::Instance().job_id);
    } else {
      /// TODO(Guyang Song): Get next job id from core worker by GCS client.
      /// Random a number to avoid repeated job ids.
      /// The repeated job ids will lead to task hang when driver connects to a existing
      /// cluster more than once.
      std::srand(std::time(nullptr));
      options.job_id = JobID::FromInt(std::rand());
    }
  }
  options.gcs_options = gcs_options;
  options.enable_logging = true;
  options.log_dir = std::move(log_dir);
  options.install_failure_signal_handler = true;
  options.node_ip_address = node_ip;
  options.node_manager_port = ConfigInternal::Instance().node_manager_port;
  options.raylet_ip_address = node_ip;
  options.driver_name = "cpp_worker";
  options.num_workers = 1;
  options.metrics_agent_port = -1;
  options.task_execution_callback = callback;
  rpc::JobConfig job_config;
  for (const auto &path : ConfigInternal::Instance().code_search_path) {
    job_config.add_code_search_path(path);
  }
  std::string serialized_job_config;
  RAY_CHECK(job_config.SerializeToString(&serialized_job_config));
  options.serialized_job_config = serialized_job_config;
  CoreWorkerProcess::Initialize(options);
}

void ProcessHelper::RayStop() {
  CoreWorkerProcess::Shutdown();
  if (ConfigInternal::Instance().redis_ip.empty()) {
    StopRayNode();
  }
}

}  // namespace internal
}  // namespace ray
