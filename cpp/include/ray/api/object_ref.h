
#pragma once

#include <ray/api/ray_config.h>
#include <ray/api/ray_runtime_holder.h>
#include <ray/api/serializer.h>

#include <memory>
#include <msgpack.hpp>
#include <utility>

#include "ray/core.h"

namespace ray {
namespace api {

/// Represents an object in the object store..
/// \param T The type of object.
template <typename T>
class ObjectRef {
 public:
  ObjectRef();
  ~ObjectRef();

  ObjectRef(const ObjectRef &rhs) { CopyAndAddRefrence(rhs.id_); }

  ObjectRef &operator=(const ObjectRef &rhs) {
    CopyAndAddRefrence(rhs.id_);
    return *this;
  }

  ObjectRef(const ObjectID &id);

  bool operator==(const ObjectRef<T> &object) const;

  /// Get a untyped ID of the object
  const ObjectID &ID() const;

  /// Get the object from the object store.
  /// This method will be blocked until the object is ready.
  ///
  /// \return shared pointer of the result.
  std::shared_ptr<T> Get() const;

  /// Make ObjectRef serializable
  MSGPACK_DEFINE(id_);

 private:
  void CopyAndAddRefrence(const ObjectID &id) {
    id_ = id;
    if (CoreWorkerProcess::IsInitialized()) {
      auto &core_worker = CoreWorkerProcess::GetCoreWorker();
      core_worker.AddLocalReference(id_);
    }
  }
  ObjectID id_;
};

// ---------- implementation ----------
template <typename T>
inline static std::shared_ptr<T> GetFromRuntime(const ObjectRef<T> &object) {
  auto packed_object = internal::RayRuntime()->Get(object.ID());
  if (ray::api::RayConfig::GetInstance()->use_ray_remote) {
    bool has_error = Serializer::HasError(packed_object->data(), packed_object->size());
    if (has_error) {
      auto tp = Serializer::Deserialize<std::tuple<int, std::string>>(
          packed_object->data(), packed_object->size(), 1);
      std::string err_msg = std::get<1>(tp);
      RAY_LOG(WARNING) << "Exception code: " << std::get<0>(tp)
                       << ", Exception message: " << err_msg;
      throw RayException(err_msg);
    }
  }

  return Serializer::Deserialize<std::shared_ptr<T>>(packed_object->data(),
                                                     packed_object->size());
}

template <typename T>
ObjectRef<T>::ObjectRef() {}

template <typename T>
ObjectRef<T>::ObjectRef(const ObjectID &id) {
  CopyAndAddRefrence(id);
}

template <typename T>
ObjectRef<T>::~ObjectRef() {
  if (CoreWorkerProcess::IsInitialized()) {
    auto &core_worker = CoreWorkerProcess::GetCoreWorker();
    core_worker.RemoveLocalReference(id_);
  }
}

template <typename T>
inline bool ObjectRef<T>::operator==(const ObjectRef<T> &object) const {
  return id_ == object.id_;
}

template <typename T>
const ObjectID &ObjectRef<T>::ID() const {
  return id_;
}

template <typename T>
inline std::shared_ptr<T> ObjectRef<T>::Get() const {
  return GetFromRuntime(*this);
}

template <>
class ObjectRef<void> {
 public:
  ObjectRef() = default;
  ~ObjectRef() {
    if (CoreWorkerProcess::IsInitialized()) {
      auto &core_worker = CoreWorkerProcess::GetCoreWorker();
      core_worker.RemoveLocalReference(id_);
    }
  }

  ObjectRef(const ObjectRef &rhs) { CopyAndAddRefrence(rhs.id_); }

  ObjectRef &operator=(const ObjectRef &rhs) {
    CopyAndAddRefrence(rhs.id_);
    return *this;
  }

  ObjectRef(const ObjectID &id) { CopyAndAddRefrence(id); }

  bool operator==(const ObjectRef<void> &object) const { return id_ == object.id_; }

  /// Get a untyped ID of the object
  const ObjectID &ID() const { return id_; }

  /// Get the object from the object store.
  /// This method will be blocked until the object is ready.
  ///
  /// \return shared pointer of the result.
  void Get() const {
    auto packed_object = internal::RayRuntime()->Get(id_);
    bool has_error = Serializer::HasError(packed_object->data(), packed_object->size());
    if (has_error) {
      auto tp = Serializer::Deserialize<std::tuple<int, std::string>>(
          packed_object->data(), packed_object->size(), 1);
      std::string err_msg = std::get<1>(tp);
      RAY_LOG(WARNING) << "Exception code: " << std::get<0>(tp)
                       << ", Exception message: " << err_msg;
      throw RayException(err_msg);
    }
  }

  /// Make ObjectRef serializable
  MSGPACK_DEFINE(id_);

 private:
  void CopyAndAddRefrence(const ObjectID &id) {
    id_ = id;
    if (CoreWorkerProcess::IsInitialized()) {
      auto &core_worker = CoreWorkerProcess::GetCoreWorker();
      core_worker.AddLocalReference(id_);
    }
  }
  ObjectID id_;
};
}  // namespace api
}  // namespace ray