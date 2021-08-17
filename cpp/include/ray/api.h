// Copyright 2020-2021 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <ray/api/actor_creator.h>
#include <ray/api/actor_handle.h>
#include <ray/api/actor_task_caller.h>
#include <ray/api/logging.h>
#include <ray/api/object_ref.h>
#include <ray/api/ray_config.h>
#include <ray/api/ray_remote.h>
#include <ray/api/ray_runtime.h>
#include <ray/api/ray_runtime_holder.h>
#include <ray/api/task_caller.h>
#include <ray/api/wait_result.h>

#include <boost/callable_traits.hpp>
#include <memory>
#include <msgpack.hpp>
#include <mutex>

namespace ray {

/// Initialize Ray runtime with config.
void Init(ray::RayConfig &config);

/// Initialize Ray runtime with config and command-line arguments.
/// If a parameter is explicitly set in command-line arguments, the parameter value will
/// be overwritten.
void Init(ray::RayConfig &config, int argc, char **argv);

/// Initialize Ray runtime with default config.
void Init();

/// Check if ray::Init has been called yet.
bool IsInitialized();

/// Shutdown Ray runtime.
void Shutdown();

/// Store an object in the object store.
///
/// \param[in] obj The object which should be stored.
/// \return ObjectRef A reference to the object in the object store.
template <typename T>
ray::ObjectRef<T> Put(const T &obj);

/// Get a single object from the object store.
/// This method will be blocked until the object is ready.
///
/// \param[in] object The object reference which should be returned.
/// \return shared pointer of the result.
template <typename T>
std::shared_ptr<T> Get(const ray::ObjectRef<T> &object);

/// Get a list of objects from the object store.
/// This method will be blocked until all the objects are ready.
///
/// \param[in] objects The object array which should be got.
/// \return shared pointer array of the result.
template <typename T>
std::vector<std::shared_ptr<T>> Get(const std::vector<ray::ObjectRef<T>> &objects);

/// Wait for a list of objects to be locally available,
/// until specified number of objects are ready, or specified timeout has passed.
///
/// \param[in] objects The object array which should be waited.
/// \param[in] num_objects The minimum number of objects to wait.
/// \param[in] timeout_ms The maximum wait time in milliseconds.
/// \return Two arrays, one containing locally available objects, one containing the
/// rest.
template <typename T>
WaitResult<T> Wait(const std::vector<ray::ObjectRef<T>> &objects, int num_objects,
                   int timeout_ms);

/// Create a `TaskCaller` for calling remote function.
/// It is used for normal task, such as ray::Task(Plus1, 1), ray::Task(Plus, 1, 2).
/// \param[in] func The function to be remote executed.
/// \param[in] args The function arguments passed by a value or ObjectRef.
/// \return TaskCaller.
template <typename F>
ray::internal::TaskCaller<F> Task(F func);

/// Generic version of creating an actor
/// It is used for creating an actor, such as: ActorCreator<Counter> creator =
/// ray::Actor(Counter::FactoryCreate<int>).Remote(1);
template <typename F>
ray::internal::ActorCreator<F> Actor(F create_func);

/// Get a handle to a global named actor.
/// Gets a handle to a global named actor with the given name. The actor must have been
/// created with global name specified.
///
/// \param[in] name The global name of the named actor.
/// \return An ActorHandle to the actor if the actor of specified name exists or an
/// empty optional object.
template <typename T>
boost::optional<ActorHandle<T>> GetGlobalActor(const std::string &actor_name);

/// Intentionally exit the current actor.
/// It is used to disconnect an actor and exit the worker.
/// \Throws RayException if the current process is a driver or the current worker is not
/// an actor.
inline void ExitActor() { ray::internal::GetRayRuntime()->ExitActor(); }

template <typename T>
std::vector<std::shared_ptr<T>> Get(const std::vector<std::string> &ids);

template <typename FuncType>
ray::internal::TaskCaller<FuncType> TaskInternal(FuncType &func);

template <typename FuncType>
ray::internal::ActorCreator<FuncType> CreateActorInternal(FuncType &func);

template <typename T>
inline static boost::optional<ActorHandle<T>> GetActorInternal(
    bool global, const std::string &actor_name);

// --------- inline implementation ------------

template <typename T>
inline std::vector<std::string> ObjectRefsToObjectIDs(
    const std::vector<ray::ObjectRef<T>> &object_refs) {
  std::vector<std::string> object_ids;
  for (auto it = object_refs.begin(); it != object_refs.end(); it++) {
    object_ids.push_back(it->ID());
  }
  return object_ids;
}

template <typename T>
inline ray::ObjectRef<T> Put(const T &obj) {
  auto buffer =
      std::make_shared<msgpack::sbuffer>(ray::internal::Serializer::Serialize(obj));
  auto id = ray::internal::GetRayRuntime()->Put(buffer);
  return ray::ObjectRef<T>(id);
}

template <typename T>
inline std::shared_ptr<T> Get(const ray::ObjectRef<T> &object) {
  return GetFromRuntime(object);
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Get(const std::vector<std::string> &ids) {
  auto result = ray::internal::GetRayRuntime()->Get(ids);
  std::vector<std::shared_ptr<T>> return_objects;
  return_objects.reserve(result.size());
  for (auto it = result.begin(); it != result.end(); it++) {
    auto obj = ray::internal::Serializer::Deserialize<std::shared_ptr<T>>((*it)->data(),
                                                                          (*it)->size());
    return_objects.push_back(std::move(obj));
  }
  return return_objects;
}

template <typename T>
inline std::vector<std::shared_ptr<T>> Get(const std::vector<ray::ObjectRef<T>> &ids) {
  auto object_ids = ObjectRefsToObjectIDs<T>(ids);
  return Get<T>(object_ids);
}

template <typename T>
inline WaitResult<T> Wait(const std::vector<ray::ObjectRef<T>> &objects, int num_objects,
                          int timeout_ms) {
  auto object_ids = ObjectRefsToObjectIDs<T>(objects);
  auto results =
      ray::internal::GetRayRuntime()->Wait(object_ids, num_objects, timeout_ms);
  std::list<ray::ObjectRef<T>> readys;
  std::list<ray::ObjectRef<T>> unreadys;
  for (size_t i = 0; i < results.size(); i++) {
    if (results[i] == true) {
      readys.emplace_back(objects[i]);
    } else {
      unreadys.emplace_back(objects[i]);
    }
  }
  return WaitResult<T>(std::move(readys), std::move(unreadys));
}

template <typename FuncType>
inline ray::internal::TaskCaller<FuncType> TaskInternal(FuncType &func) {
  ray::internal::RemoteFunctionHolder remote_func_holder(func);
  return ray::internal::TaskCaller<FuncType>(ray::internal::GetRayRuntime().get(),
                                             std::move(remote_func_holder));
}

template <typename FuncType>
inline ray::internal::ActorCreator<FuncType> CreateActorInternal(FuncType &create_func) {
  ray::internal::RemoteFunctionHolder remote_func_holder(create_func);
  return ray::internal::ActorCreator<FuncType>(ray::internal::GetRayRuntime().get(),
                                               std::move(remote_func_holder));
}

/// Normal task.
template <typename F>
ray::internal::TaskCaller<F> Task(F func) {
  return TaskInternal<F>(func);
}

/// Creating an actor.
template <typename F>
ray::internal::ActorCreator<F> Actor(F create_func) {
  return CreateActorInternal<F>(create_func);
}

template <typename T>
inline boost::optional<ActorHandle<T>> GetActorInternal(bool global,
                                                        const std::string &actor_name) {
  if (actor_name.empty()) {
    return {};
  }

  auto actor_id = ray::internal::GetRayRuntime()->GetActorId(global, actor_name);
  if (actor_id.empty()) {
    return {};
  }

  return ActorHandle<T>(actor_id);
}

template <typename T>
boost::optional<ActorHandle<T>> GetGlobalActor(const std::string &actor_name) {
  return GetActorInternal<T>(true, actor_name);
}

}  // namespace ray
