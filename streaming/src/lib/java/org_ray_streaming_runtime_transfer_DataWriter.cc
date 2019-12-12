#include "org_ray_streaming_runtime_transfer_DataWriter.h"

using namespace ray::streaming;

JNIEXPORT jlong JNICALL Java_org_ray_streaming_runtime_transfer_DataWriter_createDataWriterNative(
  JNIEnv *env, jobject this_obj,
  jlong core_worker, jobjectArray actor_id_vec,
    jobject async_func, jobject sync_func,
    jobjectArray output_queue_ids,  // byte[][]
    jlongArray seq_ids, jlong queue_size, jlongArray creator_type,
    jbyteArray fsb_conf_byte_array)
  ) {
  STREAMING_LOG(INFO) << "[JNI]: createDataWriterNative.";
  std::vector<ray::ObjectID> queue_id_vec =
      jarray_to_object_id_vec(env, output_queue_ids);
  for (auto qid : queue_id_vec) {
    STREAMING_LOG(INFO) << "output qid: " << qid.Hex();
  }
  STREAMING_LOG(INFO) << "total queue size: " << queue_size << "*" << queue_id_vec.size()
                      << "=" << queue_id_vec.size() * queue_size;
  LongVectorFromJLongArray long_array_obj(env, seq_ids);

  std::vector<uint64_t> msg_ids_vec = LongVectorFromJLongArray(env, seq_ids).data;

  std::vector<uint64_t> queue_size_vec(long_array_obj.data.size(), queue_size);

  std::vector<ray::ObjectID> remain_id_vec;

  LongVectorFromJLongArray create_types_vec(env, creator_type);
  std::vector<ray::ActorID> actor_ids = jarray_to_actor_id_vec(env, actor_id_vec);

  STREAMING_LOG(INFO) << "core_worker: " << reinterpret_cast<ray::CoreWorker *>(core_worker);
  STREAMING_LOG(INFO) << "actor_ids: " << actor_ids[0];
  ray::RayFunction af = FunctionDescriptorToRayFunction(env, async_func);
  ray::RayFunction sf = FunctionDescriptorToRayFunction(env, sync_func);
  std::vector<std::string> af_ds = af.GetFunctionDescriptor();
  std::vector<std::string> sf_ds = sf.GetFunctionDescriptor();
  for (auto &str : af_ds) {
    STREAMING_LOG(INFO) << "af_ds: " << str;
  }
  for (auto &str : sf_ds) {
    STREAMING_LOG(INFO) << "sf_ds: " << str;
  }

  const jbyte *fbs_conf_bytes = env->GetByteArrayElements(fsb_conf_byte_array, 0);
  uint32_t fbs_len = env->GetArrayLength(fsb_conf_byte_array);
  STREAMING_CHECK(fbs_conf_bytes != nullptr);
  std::shared<RuntimeContext> runtime_context = 
    std::make_shared<RuntimeContext>();
  runtime_context->SetConfig(reinterpret_cast<const uint8_t *>(fbs_conf_bytes),
                            fbs_len);
  
  auto* data_writer = new DataWriter(runtime_context);

  StreamingStatus st = data_writer->Init(queue_id_vec, actor_ids, msg_ids_vec, queue_size_vec);
  if (st != StreamingStatus::OK) {
    STREAMING_LOG(WARNING) << "DataWriter init failed.";
  } else {
    STREAMING_LOG(INFO) << "DataWriter init success";
  }

  data_writer->Run();
  return reinterpret_cast<jlong>(data_writer);
}

JNIEXPORT jlong JNICALL
Java_org_ray_streaming_runtime_transfer_DataWriter_writeMessageNative(
    JNIEnv *env, jobject, jlong writer_ptr, jlong qid_ptr, jlong address, jint size) {
  auto *data_writer = reinterpret_cast<DataWriter *>(writer_ptr);
  auto qid = *reinterpret_cast<ray::ObjectID *>(qid_ptr);
  auto data = reinterpret_cast<uint8_t *>(address);
  auto data_size = static_cast<uint32_t>(size);
  jlong result = data_writer->WriteMessageToBufferRing(qid, data, data_size,
                                                         StreamingMessageType::Message);

  if (result == 0) {
    STREAMING_LOG(INFO) << "producer interrupted, return 0.";
    throwQueueInterruptException(env, "producer interrupted.");
  }
  return result;
}

JNIEXPORT void JNICALL
Java_org_ray_streaming_runtime_transfer_DataWriter_stopProducerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  STREAMING_LOG(INFO) << "jni: stop producer.";
  DataWriter *data_writer = reinterpret_cast<DataWriter *>(ptr);
  data_writer->Stop();
}

JNIEXPORT void JNICALL
Java_org_ray_streaming_runtime_transfer_DataWriter_closeProducerNative(
    JNIEnv *env, jobject thisObj, jlong ptr) {
  DataWriter *data_writer = reinterpret_cast<DataWriter *>(ptr);
  delete data_writer;
}


JNIEXPORT void JNICALL 
Java_org_ray_streaming_runtime_transfer_DataWriter_onTransfer(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "Java_org_ray_streaming_runtime_transfer_DataWriter_onTransfer";
  WriterClient* client = reinterpret_cast<WriterClient*>(ptr);

  jbyte* buffer_bytes = env->GetByteArrayElements(bytes, 0);
  uint32_t buffer_len = env->GetArrayLength(bytes);
  if (!buffer_bytes) {
    STREAMING_LOG(ERROR) << "buffer_bytes null!";
    return;
  }
  
  std::shared_ptr<ray::LocalMemoryBuffer> buffer = 
    std::make_shared<ray::LocalMemoryBuffer>(reinterpret_cast<uint8_t*>(buffer_bytes), buffer_len);
  client->OnWriterMessage(buffer);
}

JNIEXPORT jbyteArray JNICALL 
Java_org_ray_streaming_runtime_transfer_DataWriter_onTransferSync(
    JNIEnv *env, jobject this_obj, jlong ptr, jbyteArray bytes) {
  STREAMING_LOG(INFO) << "Java_org_ray_streaming_runtime_transfer_DataWriter_onTransferSync";
  WriterClient* client = reinterpret_cast<WriterClient*>(ptr);

  jbyte* buffer_bytes = env->GetByteArrayElements(bytes, 0);
  uint32_t buffer_len = env->GetArrayLength(bytes);
  if (!buffer_bytes) {
    STREAMING_LOG(ERROR) << "buffer_bytes null!";
    return env->NewByteArray(0);
  }
  
  std::shared_ptr<ray::LocalMemoryBuffer> buffer = 
    std::make_shared<ray::LocalMemoryBuffer>(reinterpret_cast<uint8_t*>(buffer_bytes), buffer_len);
  std::shared_ptr<ray::LocalMemoryBuffer> result_buffer = client->OnWriterMessageSync(buffer);

  jbyteArray arr = env->NewByteArray(result_buffer->Size());
  env->SetByteArrayRegion(arr, 0, result_buffer->Size(), 
      reinterpret_cast<jbyte *>(result_buffer->Data()));
  return arr;
}