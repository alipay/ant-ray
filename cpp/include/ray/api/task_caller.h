
#pragma once

#include <ray/api/util.h>
#include "ray/core.h"

namespace ray {
namespace api {

template <typename ReturnType, typename F = void>
class TaskCaller {
 public:
  TaskCaller();

  TaskCaller(RayRuntime *runtime, std::string function_name)
      : runtime_(runtime), function_name_(std::move(function_name)) {}

  TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
             std::vector<std::unique_ptr<::ray::TaskArg>> &&args);

  template <typename Function, typename... Args>
  typename std::enable_if<sizeof...(Args) == 0>::type Check() {}

  template <typename Function, typename... Args>
  typename std::enable_if<sizeof...(Args) != 0>::type Check() {
    StaticCheck<Function, Args...>();
  }

  template <typename... Args>
  ObjectRef<ReturnType> Remote(Args... args) {
    if (ray::api::RayConfig::GetInstance()->use_ray_remote) {
      Check<F, Args...>();
      Arguments::WrapArgs(&args_, function_name_, args...);
    }

    auto returned_object_id = runtime_->Call(ptr_, args_);
    return ObjectRef<ReturnType>(returned_object_id);
  }

 private:
  RayRuntime *runtime_;
  RemoteFunctionPtrHolder ptr_{};
  std::string function_name_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------

template <typename ReturnType, typename F>
TaskCaller<ReturnType, F>::TaskCaller() {}

template <typename ReturnType, typename F>
TaskCaller<ReturnType, F>::TaskCaller(RayRuntime *runtime, RemoteFunctionPtrHolder ptr,
                                      std::vector<std::unique_ptr<::ray::TaskArg>> &&args)
    : runtime_(runtime), ptr_(ptr), args_(std::move(args)) {}
}  // namespace api
}  // namespace ray
