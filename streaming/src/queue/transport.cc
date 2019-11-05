#include "transport.h"
#include "utils.h"

namespace ray {
namespace streaming {

static constexpr int TASK_OPTION_RETURN_NUM_0 = 0;
static constexpr int TASK_OPTION_RETURN_NUM_1 = 1;

std::shared_ptr<Transport> TransportFactory::instance_ = nullptr;
std::mutex TransportFactory::mutex_;
const uint32_t Message::MagicNum = 0xBABA0510;

std::unique_ptr<LocalMemoryBuffer> Message::ToBytes() {
  uint8_t *bytes = nullptr;

  flatbuffers::FlatBufferBuilder fbb;
  ConstructFlatBuf(fbb);
  int64_t fbs_length = fbb.GetSize();

  queue::flatbuf::MessageType type = Type();
  size_t total_len =
      sizeof(Message::MagicNum) + sizeof(type) + sizeof(fbs_length) + fbs_length;
  if (buffer_ != nullptr) {
    total_len += buffer_->Size();
  }
  bytes = new uint8_t[total_len];
  STREAMING_CHECK(bytes != nullptr) << "allocate bytes fail.";

  uint8_t *p_cur = bytes;
  memcpy(p_cur, &Message::MagicNum, sizeof(Message::MagicNum));

  p_cur += sizeof(Message::MagicNum);
  memcpy(p_cur, &type, sizeof(type));

  p_cur += sizeof(type);
  memcpy(p_cur, &fbs_length, sizeof(fbs_length));

  p_cur += sizeof(fbs_length);
  uint8_t *fbs_bytes = fbb.GetBufferPointer();
  memcpy(p_cur, fbs_bytes, fbs_length);
  p_cur += fbs_length;

  if (buffer_ != nullptr) {
    memcpy(p_cur, buffer_->Data(), buffer_->Size());
  }

  // COPY
  std::unique_ptr<LocalMemoryBuffer> buffer =
      std::unique_ptr<LocalMemoryBuffer>(new LocalMemoryBuffer(bytes, total_len, true));
  delete bytes;
  return buffer;
}

void DataMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueDataMsg(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()), seq_id_, buffer_->Size(), raw_);
  builder.Finish(message);
}

std::shared_ptr<DataMessage> DataMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  uint64_t *fbs_length = (uint64_t *)bytes;
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueueDataMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  uint64_t length = message->length();
  bool raw = message->raw();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " seq_id:" << seq_id
                       << " queue_id:" << queue_id << " length:" << length
                       << " raw: " << raw;

  bytes += *fbs_length;
  /// COPY
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(bytes, (size_t)length, true);
  std::shared_ptr<DataMessage> data_msg = std::make_shared<DataMessage>(
      src_actor_id, dst_actor_id, queue_id, seq_id, buffer, raw);

  return data_msg;
}

void NotificationMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueNotificationMsg(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()), seq_id_);
  builder.Finish(message);
}

std::shared_ptr<NotificationMessage> NotificationMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueNotificationMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                       << " seq_id:" << seq_id;

  std::shared_ptr<NotificationMessage> notify_msg =
      std::make_shared<NotificationMessage>(src_actor_id, dst_actor_id, queue_id, seq_id);

  return notify_msg;
}

void CheckMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueCheckMsg(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()));
  builder.Finish(message);
}

std::shared_ptr<CheckMessage> CheckMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueueCheckMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  STREAMING_LOG(INFO) << "src_actor_id:" << src_actor_id
                      << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id;

  std::shared_ptr<CheckMessage> check_msg =
      std::make_shared<CheckMessage>(src_actor_id, dst_actor_id, queue_id);

  return check_msg;
}

void CheckRspMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueCheckRspMsg(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()), err_code_);
  builder.Finish(message);
}

std::shared_ptr<CheckRspMessage> CheckRspMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueueCheckRspMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  queue::flatbuf::StreamingQueueError err_code = message->err_code();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                       << " err_code:"
                       << queue::flatbuf::EnumNameStreamingQueueError(err_code);

  std::shared_ptr<CheckRspMessage> check_rsp_msg =
      std::make_shared<CheckRspMessage>(src_actor_id, dst_actor_id, queue_id, err_code);

  return check_rsp_msg;
}

void PullRequestMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueuePullRequestMsg(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()), seq_id_, async_);
  builder.Finish(message);
}

std::shared_ptr<PullRequestMessage> PullRequestMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueuePullRequestMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  bool async = message->async();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                       << " seq_id:" << seq_id << " async: " << async;

  std::shared_ptr<PullRequestMessage> pull_msg =
      std::make_shared<PullRequestMessage>(src_actor_id, dst_actor_id, queue_id, seq_id, async);

  return pull_msg;
}

void PullDataMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueuePullDataMsg(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()), first_seq_id_, seq_id_, last_seq_id_,
      buffer_->Size(), raw_);
  builder.Finish(message);
}

std::shared_ptr<PullDataMessage> PullDataMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  uint64_t *fbs_length = (uint64_t *)bytes;
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueuePullDataMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  uint64_t first_seq_id = message->first_seq_id();
  uint64_t seq_id = message->seq_id();
  uint64_t last_seq_id = message->last_seq_id();
  uint64_t length = message->length();
  bool raw = message->raw();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id
                       << " first_seq_id:" << first_seq_id << " seq_id:" << seq_id
                       << " last_seq_id:" << last_seq_id << " queue_id:" << queue_id
                       << " length:" << length;

  bytes += *fbs_length;
  /// COPY
  std::shared_ptr<LocalMemoryBuffer> buffer =
      std::make_shared<LocalMemoryBuffer>(bytes, (size_t)length, true);
  std::shared_ptr<PullDataMessage> pull_data_msg = std::make_shared<PullDataMessage>(
      src_actor_id, dst_actor_id, queue_id, first_seq_id, seq_id, last_seq_id, buffer, raw);

  return pull_data_msg;
}

void PullResponseMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueuePullResponseMsg(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()), err_code_);
  builder.Finish(message);
}

std::shared_ptr<PullResponseMessage> PullResponseMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueuePullResponseMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  queue::flatbuf::StreamingQueueError err_code = message->err_code();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id
                       << " err_code:"
                       << queue::flatbuf::EnumNameStreamingQueueError(err_code);

  std::shared_ptr<PullResponseMessage> pull_rsp_msg =
      std::make_shared<PullResponseMessage>(src_actor_id, dst_actor_id, queue_id,
                                            err_code);

  return pull_rsp_msg;
}

void ResubscribeMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueResubscribeMsg(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()));
  builder.Finish(message);
}

std::shared_ptr<ResubscribeMessage> ResubscribeMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueResubscribeMsg>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id;

  std::shared_ptr<ResubscribeMessage> resub_msg =
      std::make_shared<ResubscribeMessage>(src_actor_id, dst_actor_id, queue_id);

  return resub_msg;
}

void GetLastMsgIdMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueGetLastMsgId(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()));
  builder.Finish(message);
}

std::shared_ptr<GetLastMsgIdMessage> GetLastMsgIdMessage::FromBytes(uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message = flatbuffers::GetRoot<queue::flatbuf::StreamingQueueGetLastMsgId>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id;

  std::shared_ptr<GetLastMsgIdMessage> get_last_msg_id_msg =
      std::make_shared<GetLastMsgIdMessage>(src_actor_id, dst_actor_id, queue_id);

  return get_last_msg_id_msg;
}

void GetLastMsgIdRspMessage::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueGetLastMsgIdRsp(
      builder, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(queue_id_.Binary()), seq_id_, msg_id_, err_code_);
  builder.Finish(message);
}

std::shared_ptr<GetLastMsgIdRspMessage> GetLastMsgIdRspMessage::FromBytes(
    uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueGetLastMsgIdRsp>(bytes);
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  ObjectID queue_id = ObjectID::FromBinary(message->queue_id()->str());
  uint64_t seq_id = message->seq_id();
  uint64_t msg_id = message->msg_id();
  queue::flatbuf::StreamingQueueError err_code = message->err_code();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id << " queue_id:" << queue_id;

  std::shared_ptr<GetLastMsgIdRspMessage> get_last_msg_id_rsp_msg =
      std::make_shared<GetLastMsgIdRspMessage>(src_actor_id, dst_actor_id, queue_id,
                                               seq_id, msg_id, err_code);

  return get_last_msg_id_rsp_msg;
}

void TestInitMsg::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> queue_id_strs;
  for (auto &queue_id : queue_ids_) {
    queue_id_strs.push_back(builder.CreateString(queue_id.Binary()));
  }
  std::vector<flatbuffers::Offset<flatbuffers::String>> rescale_queue_id_strs;
  for (auto &queue_id : rescale_queue_ids_) {
    rescale_queue_id_strs.push_back(builder.CreateString(queue_id.Binary()));
  }
  auto message = queue::flatbuf::CreateStreamingQueueTestInitMsg(
      builder, role_, builder.CreateString(actor_id_.Binary()),
      builder.CreateString(peer_actor_id_.Binary()),
      builder.CreateString(actor_handle_serialized_),
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(queue_id_strs),
      builder.CreateVector<flatbuffers::Offset<flatbuffers::String>>(rescale_queue_id_strs),
      builder.CreateString(test_suite_name_), builder.CreateString(test_name_), param_);
  builder.Finish(message);
}

std::shared_ptr<TestInitMsg> TestInitMsg::FromBytes(
    uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueTestInitMsg>(bytes);
  queue::flatbuf::StreamingQueueTestRole role = message->role();
  ActorID src_actor_id = ActorID::FromBinary(message->src_actor_id()->str());
  ActorID dst_actor_id = ActorID::FromBinary(message->dst_actor_id()->str());
  std::string actor_handle_serialized = message->actor_handle()->str();
  std::vector<ObjectID> queue_ids;
  const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *queue_id_strs = message->queue_ids();
  for (auto it = queue_id_strs->begin(); it != queue_id_strs->end(); it++) {
    queue_ids.push_back(ObjectID::FromBinary(it->str()));
  }
  std::vector<ObjectID> rescale_queue_ids;
  const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *rescale_queue_id_strs = message->rescale_queue_ids();
  for (auto it = rescale_queue_id_strs->begin(); it != rescale_queue_id_strs->end(); it++) {
    rescale_queue_ids.push_back(ObjectID::FromBinary(it->str()));
  }
  std::string test_suite_name = message->test_suite_name()->str();
  std::string test_name = message->test_name()->str();
  uint64_t param = message->param();
  STREAMING_LOG(DEBUG) << "src_actor_id:" << src_actor_id
                       << " dst_actor_id:" << dst_actor_id
                       << " test_suite_name: " << test_suite_name
                       << " test_name: " << test_name;

  std::shared_ptr<TestInitMsg> test_init_msg =
      std::make_shared<TestInitMsg>(role, src_actor_id, dst_actor_id, actor_handle_serialized, queue_ids, rescale_queue_ids, test_suite_name, test_name, param);

  return test_init_msg;
}

void TestCheckStatusRspMsg::ConstructFlatBuf(flatbuffers::FlatBufferBuilder &builder) {
  auto message = queue::flatbuf::CreateStreamingQueueTestCheckStatusRspMsg(
      builder, builder.CreateString(test_name_), status_);
  builder.Finish(message);
}

std::shared_ptr<TestCheckStatusRspMsg> TestCheckStatusRspMsg::FromBytes(
    uint8_t *bytes) {
  bytes += sizeof(uint32_t) + sizeof(queue::flatbuf::MessageType);
  bytes += sizeof(uint64_t);

  /// TODO: Verify buffer
  auto message =
      flatbuffers::GetRoot<queue::flatbuf::StreamingQueueTestCheckStatusRspMsg>(bytes);
  std::string test_name = message->test_name()->str();
  bool status = message->status();
  STREAMING_LOG(DEBUG) << "test_name: " << test_name
                       << " status: " << status;

  std::shared_ptr<TestCheckStatusRspMsg> test_check_msg =
      std::make_shared<TestCheckStatusRspMsg>(test_name, status);

  return test_check_msg;
}

void DirectCallTransport::Send(std::unique_ptr<LocalMemoryBuffer> buffer) {
  /// org.ray.streaming.runtime.JobWorker
  /// onStreamingTransfer
  /// ()V
  STREAMING_LOG(INFO) << "DirectCallTransport::Send buffer size: " << buffer->Size();
  //  RayFunction func{ray::Language::JAVA,
  //  {"org.ray.streaming.runtime.JobWorker",
  //                                         "onStreamingTransfer", "()V"}};
  //  RayFunction func{ray::Language::JAVA, {"org.ray.streaming.runtime.JobWorker",
  //                                         "onStreamingTransfer", "([B)V"}};
  std::unordered_map<std::string, double> resources;
  TaskOptions options{TASK_OPTION_RETURN_NUM_0, resources};

  char meta_data[3] = {'R', 'A', 'W'};
  std::shared_ptr<LocalMemoryBuffer> meta =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, 3, true);

  std::vector<TaskArg> args;
  args.emplace_back(
      TaskArg::PassByValue(std::make_shared<RayObject>(std::move(buffer), meta, true)));

  STREAMING_CHECK(core_worker_ != nullptr);
  // STREAMING_CHECK(peer_actor_handle_ != nullptr);
  std::vector<ObjectID> return_ids;
  STREAMING_LOG(INFO) << "DirectCallTransport::Send before peer_actor_id_" << peer_actor_id_;
  STREAMING_LOG(INFO) << "is direct call actor";
  ray::Status st = core_worker_->SubmitActorTask(peer_actor_id_, async_func_, args,
                                                         options, &return_ids);
  if (st.ok()) {
    STREAMING_LOG(INFO) << "SubmitActorTask success.";
  } else {
    STREAMING_LOG(INFO) << "SubmitActorTask fail. " << st;
  }
}

std::shared_ptr<LocalMemoryBuffer> DirectCallTransport::SendForResult(
    std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms) {
  STREAMING_LOG(INFO) << "DirectCallTransport::SendForResult buffer size: "
                      << buffer->Size();
  //  RayFunction func{ray::Language::JAVA, {"org.ray.streaming.runtime.JobWorker",
  //                                         "onStreamingTransferSync", "([B)[B"}};
  std::unordered_map<std::string, double> resources;
  TaskOptions options{TASK_OPTION_RETURN_NUM_1, resources};

  char meta_data[3] = {'R', 'A', 'W'};
  std::shared_ptr<LocalMemoryBuffer> meta =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, 3, true);

  std::vector<TaskArg> args;
  args.emplace_back(
      TaskArg::PassByValue(std::make_shared<RayObject>(buffer, meta, true)));

  STREAMING_CHECK(core_worker_ != nullptr);
  // STREAMING_CHECK(peer_actor_handle_ != nullptr);
  std::vector<ObjectID> return_ids;
  STREAMING_LOG(DEBUG) << "DirectCallTransport::SendForResult before";
  STREAMING_LOG(DEBUG) << "is direct call actor";
  ray::Status st = core_worker_->SubmitActorTask(peer_actor_id_, sync_func_, args,
                                                         options, &return_ids);
  if (st.ok()) {
    STREAMING_LOG(DEBUG) << "SubmitActorTask success.";
  } else {
    STREAMING_LOG(DEBUG) << "SubmitActorTask fail.";
  }

  std::vector<bool> wait_results;
  std::vector<std::shared_ptr<RayObject>> results;
  Status wait_st = core_worker_->Wait(return_ids, 1, timeout_ms, &wait_results);
  if (!wait_st.ok()) {
    STREAMING_LOG(ERROR) << "Wait fail.";
    return nullptr;
  }
  STREAMING_CHECK(wait_results.size() >= 1);
  // STREAMING_CHECK(wait_results[0]);
  if (!wait_results[0]) {
    STREAMING_LOG(WARNING) << "Wait direct call fail.";
    return nullptr;
  }

  Status get_st = core_worker_->Get(return_ids, -1, &results);
  if (!get_st.ok()) {
    STREAMING_LOG(ERROR) << "Get fail.";
    return nullptr;
  }
  STREAMING_CHECK(results.size() >= 1);
  if (results[0]->IsException()) {
    STREAMING_LOG(INFO) << "peer actor may has exceptions, should retry.";
    return nullptr;
  }
  STREAMING_CHECK(results[0]->HasData());
  STREAMING_LOG(DEBUG) << "SendForResult result[0] DataSize: " << results[0]->GetSize();
  /// TODO: size 4 means byte[] array size 1, we will remove this by adding flatbuf
  /// command.
  if (results[0]->GetSize() == 4) {
    STREAMING_LOG(WARNING) << "peer actor may not ready yet, should retry.";
    return nullptr;
  }

  std::shared_ptr<Buffer> result_buffer = results[0]->GetData();
  std::shared_ptr<LocalMemoryBuffer> return_buffer = std::make_shared<LocalMemoryBuffer>(
      result_buffer->Data(), result_buffer->Size(), true);
  return return_buffer;
}

std::shared_ptr<LocalMemoryBuffer> DirectCallTransport::SendForResultWithRetry(
    std::unique_ptr<LocalMemoryBuffer> buffer, int retry_cnt, int64_t timeout_ms) {
  STREAMING_LOG(INFO) << "SendForResultWithRetry retry_cnt: " << retry_cnt
                      << " timeout_ms: " << timeout_ms;
  std::shared_ptr<LocalMemoryBuffer> buffer_shared = std::move(buffer);
  for (int cnt=0; cnt<retry_cnt; cnt++) {
    auto result = SendForResult(buffer_shared, timeout_ms);
    if (result != nullptr) {
      return result;
    }
  }

  STREAMING_LOG(WARNING) << "SendForResultWithRetry fail after retry.";
  return nullptr;
}

std::shared_ptr<LocalMemoryBuffer> DirectCallTransport::Recv() {
  STREAMING_CHECK(false) << "Should not be called.";
  return nullptr;
}

}  // namespace streaming
}  // namespace ray
