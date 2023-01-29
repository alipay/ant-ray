#ifndef RAY_STREAMING_JNI_COMMON_H
#define RAY_STREAMING_JNI_COMMON_H

#include <jni.h>

#include <string>

#include "data_writer.h"
#include "ray/core_worker/common.h"

#define CURRENT_JNI_VERSION JNI_VERSION_1_8

/// DirectByteBuffer class. Global reference can be used in multi-threads
extern jclass java_direct_buffer_class;
/// address field of DirectByteBuffer
extern jfieldID java_direct_buffer_address;
/// capacity field of DirectByteBuffer
extern jfieldID java_direct_buffer_capacity;

static inline jclass FindClass(JNIEnv *env, const char *class_name) {
  jclass local = env->FindClass(class_name);
  STREAMING_CHECK(local) << "Can't find java class " << class_name;
  auto ret = (jclass)env->NewGlobalRef(local);
  env->DeleteLocalRef(local);
  return ret;
}

class UniqueIdFromJByteArray {
 private:
  JNIEnv *_env;
  jbyteArray _bytes;
  jbyte *b;

 public:
  ray::ObjectID PID;

  UniqueIdFromJByteArray(JNIEnv *env, jbyteArray wid) : _env(env), _bytes(wid) {
    b = reinterpret_cast<jbyte *>(_env->GetByteArrayElements(_bytes, nullptr));
    PID = ray::ObjectID::FromBinary(
        std::string(reinterpret_cast<const char *>(b), ray::ObjectID::Size()));
  }

  ~UniqueIdFromJByteArray() { _env->ReleaseByteArrayElements(_bytes, b, 0); }
};

class RawDataFromJByteArray {
 private:
  JNIEnv *_env;
  jbyteArray _bytes;

 public:
  uint8_t *data;
  uint32_t data_size;

  RawDataFromJByteArray(JNIEnv *env, jbyteArray bytes) : _env(env), _bytes(bytes) {
    data_size = _env->GetArrayLength(_bytes);
    jbyte *b = reinterpret_cast<jbyte *>(_env->GetByteArrayElements(_bytes, nullptr));
    data = reinterpret_cast<uint8_t *>(b);
  }

  ~RawDataFromJByteArray() {
    _env->ReleaseByteArrayElements(_bytes, reinterpret_cast<jbyte *>(data), 0);
  }
};

class StringFromJString {
 private:
  JNIEnv *_env;
  const char *j_str;
  jstring jni_str;

 public:
  std::string str;

  StringFromJString(JNIEnv *env, jstring jni_str_) : _env(env), jni_str(jni_str_) {
    j_str = env->GetStringUTFChars(jni_str, nullptr);
    str = std::string(j_str);
  }

  ~StringFromJString() { _env->ReleaseStringUTFChars(jni_str, j_str); }
};

class LongVectorFromJLongArray {
 private:
  JNIEnv *_env;
  jlongArray long_array;
  jlong *long_array_ptr = nullptr;

 public:
  std::vector<uint64_t> data;

  LongVectorFromJLongArray(JNIEnv *env, jlongArray long_array_)
      : _env(env), long_array(long_array_) {
    long_array_ptr = env->GetLongArrayElements(long_array, nullptr);
    jsize seq_id_size = env->GetArrayLength(long_array);
    data = std::vector<uint64_t>(long_array_ptr, long_array_ptr + seq_id_size);
  }

  ~LongVectorFromJLongArray() {
    _env->ReleaseLongArrayElements(long_array, long_array_ptr, 0);
  }
};

std::vector<ray::ObjectID> jarray_to_plasma_object_id_vec(JNIEnv *env, jobjectArray jarr);

jint throwRuntimeException(JNIEnv *env, const char *message);
jint throwQueueInitException(JNIEnv *env, const char *message,
                             const std::vector<ray::ObjectID> &abnormal_queues);
jint throwQueueInterruptException(JNIEnv *env, const char *message);
std::shared_ptr<ray::RayFunction> FunctionDescriptorToRayFunction(
    JNIEnv *env, jobject functionDescriptor);
void FunctionDescriptorListToRayFunctionVector(
    JNIEnv *env, jobject java_list,
    std::vector<std::shared_ptr<ray::RayFunction>> *native_vector);
void ParseStreamingQueueInitParameters(
    JNIEnv *env, jobject param_obj,
    std::vector<ray::streaming::StreamingQueueInitialParameter> &parameter_vec);

jclass LoadClass(JNIEnv *env, const char *class_name);

#define GET_JCLASS(env, classname, clazz)     \
  static jclass clazz = nullptr;              \
  if (!clazz) {                               \
    jclass local = env->FindClass(classname); \
    clazz = (jclass)env->NewGlobalRef(local); \
    env->DeleteLocalRef(local);               \
  }

#define GET_JMETHOD(env, clazz, method_id, methodname, sig) \
  static jmethodID method_id = nullptr;                     \
  if (!method_id) {                                         \
    method_id = env->GetMethodID(clazz, methodname, sig);   \
  }

jstring NativeStringTOJavaString(JNIEnv *env, const std::string &native_str);
#endif  // RAY_STREAMING_JNI_COMMON_H
