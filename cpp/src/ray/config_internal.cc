#include <boost/dll/runtime_symbol_info.hpp>
#include "gflags/gflags.h"

#include "config_internal.h"

DEFINE_string(ray_address, "", "The address of the Ray cluster to connect to.");

DEFINE_string(ray_redis_password, "",
              "Prevents external clients without the password from connecting to Redis "
              "if provided.");

DEFINE_string(ray_code_search_path, "",
              "The search path of application's dynamic libraries.");

DEFINE_string(ray_job_id, "", "Assigned job id.");

DEFINE_int32(ray_node_manager_port, 62665, "The port to use for the node manager.");

DEFINE_string(ray_raylet_socket_name, "",
              "It will specify the socket name used by the raylet if provided.");

DEFINE_string(ray_plasma_store_socket_name, "",
              "It will specify the socket name used by the plasma store if provided.");

DEFINE_string(ray_session_dir, "", "The path of this session.");

DEFINE_string(ray_logs_dir, "", "Logs dir for workers.");

DEFINE_string(ray_node_ip_address, "", "The ip address for this node.");

namespace ray {
namespace api {

void ConfigInternal::Init(RayConfig &config, int *argc, char ***argv) {
  if (!config.address.empty()) {
    SetRedisAddress(config.address);
  }
  run_mode = config.local_mode ? RunMode::SINGLE_PROCESS : RunMode::CLUSTER;
  if (!config.code_search_path.empty()) {
    code_search_path = config.code_search_path;
  }
  if (config.redis_password_) {
    redis_password = *config.redis_password_;
  }
  if (argc != nullptr && argv != nullptr) {
    // Parse config from command line.
    gflags::ParseCommandLineFlags(argc, argv, true);

    if (!FLAGS_ray_code_search_path.empty()) {
      // Code search path like this "/path1/xxx.so:/path2".
      code_search_path.clear();
      std::string separator = ":";
      unsigned int start = 0;
      auto end = FLAGS_ray_code_search_path.find(separator);
      if (end == std::string::npos) {
        code_search_path.emplace_back(FLAGS_ray_code_search_path);
      } else {
        while (end != std::string::npos) {
          code_search_path.emplace_back(
              FLAGS_ray_code_search_path.substr(start, end - start));
          start = end + separator.length();
          end = FLAGS_ray_code_search_path.find(separator, start);
        }
      }
    }
    if (!FLAGS_ray_address.empty()) {
      SetRedisAddress(FLAGS_ray_address);
    }
    google::CommandLineFlagInfo info;
    // Don't rewrite `ray_redis_password` when it is not set in the command line.
    if (GetCommandLineFlagInfo("ray_redis_password", &info) && !info.is_default) {
      redis_password = FLAGS_ray_redis_password;
    }
    if (!FLAGS_ray_job_id.empty()) {
      job_id = FLAGS_ray_job_id;
    }
    node_manager_port = FLAGS_ray_node_manager_port;
    if (!FLAGS_ray_raylet_socket_name.empty()) {
      raylet_socket_name = FLAGS_ray_raylet_socket_name;
    }
    if (!FLAGS_ray_plasma_store_socket_name.empty()) {
      plasma_store_socket_name = FLAGS_ray_plasma_store_socket_name;
    }
    if (!FLAGS_ray_session_dir.empty()) {
      session_dir = FLAGS_ray_session_dir;
    }
    if (!FLAGS_ray_logs_dir.empty()) {
      logs_dir = FLAGS_ray_logs_dir;
    }
    if (!FLAGS_ray_node_ip_address.empty()) {
      node_ip_address = FLAGS_ray_node_ip_address;
    }
    gflags::ShutDownCommandLineFlags();
  }
  if (worker_type == WorkerType::DRIVER) {
    if (run_mode == RunMode::CLUSTER && code_search_path.size() == 0) {
      auto program_path = boost::dll::program_location().parent_path();
      RAY_LOG(WARNING) << "No code search path found yet. "
                       << "The program location path " << program_path
                       << " will be added for searching dynamic libraries by default."
                       << "And you can add some search paths by '--ray-code-search-path'";
      code_search_path.emplace_back(program_path.string());
    }
  }
};

void ConfigInternal::SetRedisAddress(const std::string address) {
  auto pos = address.find(':');
  RAY_CHECK(pos != std::string::npos);
  redis_ip = address.substr(0, pos);
  redis_port = std::stoi(address.substr(pos + 1, address.length()));
}
}  // namespace api
}  // namespace ray