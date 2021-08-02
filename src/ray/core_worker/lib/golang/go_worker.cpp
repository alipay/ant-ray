#include "go_worker.h"

#include <stdint.h>

#include <iostream>

#include "ray/core_worker/core_worker.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"

using namespace std;

__attribute__((visibility("default"))) void go_worker_Initialize(
    int workerMode, char *store_socket, char *raylet_socket, char *log_dir,
    char *node_ip_address, int node_manager_port, char *raylet_ip_address,
    char *driver_name, int jobId, char *redis_address, int redis_port,
    char *redis_password) {
  SayHello((char *)"have_fun friends!");
  std::string serialized_job_config = "";
  ray::CoreWorkerOptions options;
  options.worker_type = static_cast<ray::WorkerType>(workerMode);
  options.language = ray::Language::GOLANG;
  options.store_socket = store_socket;
  options.raylet_socket = raylet_socket;
  options.job_id = ray::JobID::FromInt(jobId);
  options.gcs_options =
      ray::gcs::GcsClientOptions(redis_address, redis_port, redis_password);
  options.enable_logging = true;
  options.log_dir = log_dir;
  // TODO (kfstorm): JVM would crash if install_failure_signal_handler was set to true
  options.install_failure_signal_handler = false;
  options.node_ip_address = node_ip_address;
  options.node_manager_port = node_manager_port;
  options.raylet_ip_address = raylet_ip_address;
  options.driver_name = driver_name;
  //  options.task_execution_callback = task_execution_callback;
  //  options.on_worker_shutdown = on_worker_shutdown;
  //  options.gc_collect = gc_collect;
  options.ref_counting_enabled = true;
  options.num_workers = 1;
  options.serialized_job_config = serialized_job_config;
  options.metrics_agent_port = -1;
  ray::CoreWorkerProcess::Initialize(options);
}

__attribute__((visibility("default"))) void *go_worker_CreateGlobalStateAccessor(
    char *redis_address, char *redis_password) {
  ray::gcs::GlobalStateAccessor *gcs_accessor =
      new ray::gcs::GlobalStateAccessor(redis_address, redis_password);
  return gcs_accessor;
}

__attribute__((visibility("default"))) bool go_worker_GlobalStateAccessorConnet(void *p) {
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  return gcs_accessor->Connect();
}

__attribute__((visibility("default"))) int go_worker_GetNextJobID(void *p) {
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  auto job_id = gcs_accessor->GetNextJobID();
  return job_id.ToInt();
}

__attribute__((visibility("default"))) char *go_worker_GlobalStateAccessorGetInternalKV(
    void *p, char *key) {
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  auto value = gcs_accessor->GetInternalKV(key);
  if (value != nullptr) {
    std::string *v = value.release();
    int len = strlen(v->c_str());
    char *result = (char *)malloc(len + 1);
    std::memcpy(result, v->c_str(), len);
    return result;
  }
  return nullptr;
}

__attribute__((visibility("default"))) int go_worker_GetNodeToConnectForDriver(
    void *p, char *node_ip_address, char **result) {
  auto *gcs_accessor = static_cast<ray::gcs::GlobalStateAccessor *>(p);
  std::string node_to_connect;
  auto status =
      gcs_accessor->GetNodeToConnectForDriver(node_ip_address, &node_to_connect);
  if (!status.ok()) {
    RAY_LOG(FATAL) << "Failed to get node to connect for driver:" << status.message();
    return 0;
  }
  int result_length = strlen(node_to_connect.c_str());
  *result = (char *)malloc(result_length + 1);
  memcpy(*result, node_to_connect.c_str(), result_length);
  return result_length;
}

__attribute__((visibility("default"))) int go_worker_CreateActor(char *type_name,
                                                                 char **result) {
  std::vector<std::string> function_descriptor_list = {type_name};
  ray::FunctionDescriptor function_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(ray::rpc::GOLANG,
                                                 function_descriptor_list);
  ActorID actor_id;
  ray::RayFunction ray_function = ray::RayFunction(ray::rpc::GOLANG, function_descriptor);
  // TODO
  std::string full_name = "";
  std::string ray_namespace = "";
  ray::ActorCreationOptions actor_creation_options{
      0,
      0,  // TODO: Allow setting max_task_retries from Java.
      static_cast<int>(1),
      {},
      {},
      {},
      /*is_detached=*/false,
      full_name,
      ray_namespace,
      /*is_asyncio=*/false};
  auto status = ray::CoreWorkerProcess::GetCoreWorker().CreateActor(
      ray_function, {}, actor_creation_options,
      /*extension_data*/ "", &actor_id);
  if (!status.ok()) {
    RAY_LOG(FATAL) << "Failed to create actor:" << status.message()
                   << " for:" << type_name;
    return 0;
  }
  int result_length = actor_id.Size();
  *result = (char *)malloc(result_length + 1);
  memcpy(*result, actor_id.Data(), result_length);
  return result_length;
}

__attribute__((visibility("default"))) int go_worker_SubmitActorTask(char *actor_id,
                                                                     char *method_name,
                                                                     char ***return_ids) {
  auto actor_id_obj = ActorID::FromBinary(std::string(actor_id));
  std::vector<std::string> function_descriptor_list = {method_name};
  ray::FunctionDescriptor function_descriptor =
      ray::FunctionDescriptorBuilder::FromVector(ray::rpc::GOLANG,
                                                 function_descriptor_list);

  ray::RayFunction ray_function = ray::RayFunction(ray::rpc::GOLANG, function_descriptor);
  std::vector<ObjectID> return_ids;
  ray::CoreWorkerProcess::GetCoreWorker().SubmitActorTask(actor_id_obj, ray_function, {},
                                                          {}, &return_ids);
  return 0;
}
