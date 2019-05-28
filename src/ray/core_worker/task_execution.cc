#include "task_execution.h"

namespace ray {

void CoreWorkerTaskExecutionInterface::Start(const TaskExecutor &executor) {

  auto &context = core_worker_->GetContext();
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
      context.GetCurrentTaskID(), context.GetNextTaskIndex());

  while (true) {
    const auto &tasks = GetTasks();

    for (const auto &spec : tasks) {
      context_.SetCurrentTask(spec);

      RayFunction func{ spec.GetLanguage(), spec.FunctionDescriptor() };

      std::vector<Arg> args;
      RAY_CHECK_OK(GetArgsFromObjectStore(spec, &args)); 

      auto status = executor(func, args);
      if (status.ok()) {
         // TODO:
         // 1. Check and handle failure.
         // 2. Save or load checkpoint. 
      }

    }
  }
}

Status CoreWorkerTaskExecutionInterface::BuildArgsForExecutor(
    const TaskSpecification &spec, std::vector<Arg> *args) {
  
  auto num_args = spec.NumArgs();
  (*args).resize(num_args);

  std::vector<ObjectID> object_ids_to_fetch;
  std::vector<int> indices;

  for (int i = 0; i < spec.NumArgs(); ++i) {
    int count = spec.ArgIdCount(i);
    if (count > 0) {
      // pass by reference.
      RAY_CHECK(count == 1);
      object_ids_to_fetch.push_back(spec.ArgId(i, 0));
      indices.push_back(i);
    } else {
      // pass by value.
      (*args)[i] = std::make_shared<LocalMemoryBuffer>({ spec.ArgVal(i), spec.ArgValLength(i) });
    }
  } 

  std::vector<Buffer> results;
  auto status = core_worker_->object_interface_.Get(object_ids_to_fetch, -1, &results);
  if (status.ok()) {
    for (int i = 0; i < results.size(); i++) {
      (*args)[indices[i]] = results[i];
    }
  }

  return status;
}

}  // namespace ray
