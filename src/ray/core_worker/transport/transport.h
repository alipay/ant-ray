#ifndef RAY_CORE_WORKER_TRANSPORT_H
#define RAY_CORE_WORKER_TRANSPORT_H

#include <list>

#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/store_provider/store_provider.h"

namespace ray {

namespace rpc {
  class GrpcService;
}
/// Interfaces for task submitter and receiver. They are separate classes but should be
/// used in pairs - one type of task submitter should be used together with task
/// with the same type, so these classes are put together in this same file.
///
/// Task submitter/receiver should inherit from these classes and provide implementions
/// for the methods. The actual task submitter/receiver can submit/get tasks via raylet,
/// or directly to/from another worker.

/// This class is responsible to submit tasks.
class CoreWorkerTaskSubmitter {
 public:
  /// Submit a task for execution.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpecification &task_spec) = 0;

  /// Check if a task has finished.
  ///
  /// \param[in] task_id The ID of the task.
  /// \return If the task has finished.
  virtual bool ShouldWaitTask(const TaskID &task_id) const = 0;

  /// Get the store provider type for return objects.
  ///
  /// \return Store provider type used.
  /// Note that we currently have a limitation that a TaskSubmitter
  /// must use a single store provider for all the return objects.
  virtual StoreProviderType GetStoreProviderTypeForReturnObject() const = 0;

  virtual ~CoreWorkerTaskSubmitter() {}
};

/// This class receives tasks for execution.
class CoreWorkerTaskReceiver {
 public:
  using TaskHandler =
      std::function<Status(const TaskSpecification &task_spec,
                           std::vector<std::shared_ptr<RayObject>> *results)>;

  virtual ~CoreWorkerTaskReceiver() {}

  /// Return the underlying rpc service.
  virtual rpc::GrpcService &GetRpcService() = 0;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TRANSPORT_H
