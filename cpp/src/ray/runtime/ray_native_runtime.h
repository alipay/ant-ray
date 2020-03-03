
#pragma once

#include <ray/runtime/ray_runtime.h>

namespace ray {

class RayNativeRuntime : public RayRuntime {
  friend class RayRuntime;

 private:
  RayNativeRuntime(std::shared_ptr<RayConfig> config);
};

}  // namespace ray