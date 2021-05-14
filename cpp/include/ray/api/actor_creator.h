
#pragma once

#include <ray/api/actor_handle.h>
#include "ray/core.h"

namespace ray {
namespace api {

template <typename F>
using GetActorType = absl::remove_pointer_t<boost::callable_traits::return_type_t<F>>;

template <typename F>
class ActorCreator {
 public:
  ActorCreator() {}

  ActorCreator(RayRuntime *runtime, RemoteFunctionHolder ptr)
      : runtime_(runtime), ptr_(ptr) {}

  template <typename... Args>
  ActorHandle<GetActorType<F>> Remote(Args... args);

 private:
  RayRuntime *runtime_;
  RemoteFunctionHolder ptr_;
  std::vector<std::unique_ptr<::ray::TaskArg>> args_;
};

// ---------- implementation ----------
template <typename F>
template <typename... Args>
ActorHandle<GetActorType<F>> ActorCreator<F>::Remote(Args... args) {
  StaticCheck<F, Args...>();
  Arguments::WrapArgs(&args_, args...);
  auto returned_actor_id = runtime_->CreateActor(ptr_, args_);
  return ActorHandle<GetActorType<F>>(returned_actor_id);
}
}  // namespace api
}  // namespace ray
