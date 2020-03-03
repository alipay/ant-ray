
#pragma once

#include <ray/api/uniqueId.h>
#include <ray/core.h>
#include <unordered_map>
#include <ray/runtime/ray_runtime.h>

namespace ray {

class RayDevRuntime : public RayRuntime {
  friend class RayRuntime;

 private:
  static std::unordered_map<UniqueId, char *> _actors;

  RayDevRuntime(std::shared_ptr<RayConfig> config);

  char *get_actor_ptr(const UniqueId &id);

  std::unique_ptr<UniqueId> create(remote_function_ptr_holder &fptr,
                                   std::vector< ::ray::blob> &&args);
};

}  // namespace ray