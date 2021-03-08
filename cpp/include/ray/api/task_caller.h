
#pragma once

#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr)
      : runtime_(runtime), ptr_(ptr) {}

  TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
             std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  template <typename... Args>
  ObjectRef<ReturnType> Remote(Args... args) {
    if (ray::api::RayConfig::GetInstance()->use_ray_remote) {
      auto tp = std::make_tuple(ptr_.function_name, args...);
      Arguments::WrapArgs(&args_, tp);
    }

    auto returned_object_id = runtime_->Call(ptr_, args_);
    return ObjectRef<ReturnType>(returned_object_id);
  }

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ReturnType>
TaskCaller<ReturnType>::TaskCaller() {}

template <typename ReturnType>
TaskCaller<ReturnType>::TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
                                   std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), ptr_(ptr), args_(std::move(args)) {}
}  // namespace api
}  // namespace ray
