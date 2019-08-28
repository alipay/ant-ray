#include "ray/core_worker/core_worker.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

CoreWorker::CoreWorker(
    const WorkerType worker_type, const Language language,
    const std::string &store_socket, const std::string &raylet_socket,
    const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
    const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback,
    int num_workers)
    : worker_type_(worker_type),
      language_(language),
      store_socket_(store_socket),
      raylet_socket_(raylet_socket),
      worker_context_(worker_type, job_id),
      io_work_(io_service_) {
  bool use_asio_rpc = RayConfig::instance().use_asio_rpc_for_worker();

  // Initialize gcs client
  gcs_client_ =
      std::unique_ptr<gcs::RedisGcsClient>(new gcs::RedisGcsClient(gcs_options));
  RAY_CHECK_OK(gcs_client_->Connect(io_service_));

  InitializeStoreProviders();
  InitializeTaskSubmitters(use_asio_rpc);

  object_interface_ = std::unique_ptr<CoreWorkerObjectInterface>(
      new CoreWorkerObjectInterface(worker_context_, store_providers_, task_submitters_));
  task_interface_ = std::unique_ptr<CoreWorkerTaskInterface>(
      new CoreWorkerTaskInterface(worker_context_, task_submitters_));

  int rpc_server_port = 0;
  std::vector<WorkerID> worker_ids;
  if (worker_type_ == WorkerType::WORKER) {
    RAY_CHECK(execution_callback != nullptr);
    RAY_CHECK(num_workers > 0);
    for (int i = 0; i < num_workers; i++) {
      worker_ids.emplace_back(WorkerID::FromRandom());
    }
    task_execution_interface_ = std::unique_ptr<CoreWorkerTaskExecutionInterface>(
        new CoreWorkerTaskExecutionInterface(worker_context_, worker_ids, raylet_client_,
                                             store_providers_, execution_callback,
                                             io_service_, use_asio_rpc));
    rpc_server_port = task_execution_interface_->worker_server_->GetPort();
  } else {
    RAY_CHECK(num_workers == 1);
    worker_ids.emplace_back(ComputeDriverIdFromJob(job_id));
  }
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this can be changed later
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  raylet_client_ = std::unique_ptr<RayletClient>(new RayletClient(
      raylet_socket_, worker_ids, (worker_type_ == ray::WorkerType::WORKER),
      worker_context_.GetCurrentJobID(), language_, rpc_server_port));

  io_thread_ = std::thread(&CoreWorker::StartIOService, this);
}

CoreWorker::~CoreWorker() {
  gcs_client_->Disconnect();
  io_service_.stop();
  io_thread_.join();
  if (task_execution_interface_) {
    task_execution_interface_->Stop();
  }
  if (raylet_client_) {
    RAY_IGNORE_EXPR(raylet_client_->Disconnect());
  }
}

void CoreWorker::StartIOService() { io_service_.run(); }

void CoreWorker::InitializeStoreProviders() {
  memory_store_ = std::make_shared<CoreWorkerMemoryStore>();

  store_providers_.emplace(StoreProviderType::LOCAL_PLASMA,
                           CreateStoreProvider(StoreProviderType::LOCAL_PLASMA));
  store_providers_.emplace(StoreProviderType::PLASMA,
                           CreateStoreProvider(StoreProviderType::PLASMA));
  store_providers_.emplace(StoreProviderType::MEMORY,
                           CreateStoreProvider(StoreProviderType::MEMORY));
}

std::unique_ptr<CoreWorkerStoreProvider> CoreWorker::CreateStoreProvider(
    StoreProviderType type) {
  switch (type) {
  case StoreProviderType::LOCAL_PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerLocalPlasmaStoreProvider(store_socket_));
    break;
  case StoreProviderType::PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerPlasmaStoreProvider(store_socket_, raylet_client_));
    break;
  case StoreProviderType::MEMORY:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerMemoryStoreProvider(memory_store_));
    break;
  default:
    // Should never reach here.
    RAY_LOG(FATAL) << "unknown store provider type " << static_cast<int>(type);
    return nullptr;
  }
}

void CoreWorker::InitializeTaskSubmitters(bool use_asio_rpc) {
  // Add all task submitters.
  task_submitters_.emplace(TaskTransportType::RAYLET,
                           std::unique_ptr<CoreWorkerRayletTaskSubmitter>(
                               new CoreWorkerRayletTaskSubmitter(raylet_client_)));

  task_submitters_.emplace(
      TaskTransportType::DIRECT_ACTOR,
      use_asio_rpc ? std::unique_ptr<CoreWorkerDirectActorTaskSubmitter>(
                         new DirectActorAsioTaskSubmitter(
                             io_service_, worker_context_, *gcs_client_,
                             CreateStoreProvider(StoreProviderType::MEMORY)))
                   : std::unique_ptr<CoreWorkerDirectActorTaskSubmitter>(
                         new DirectActorGrpcTaskSubmitter(
                             io_service_, worker_context_, *gcs_client_,
                             CreateStoreProvider(StoreProviderType::MEMORY))));
}

}  // namespace ray
