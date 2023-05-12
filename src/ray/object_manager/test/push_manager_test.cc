// Copyright 2017 The Ray Authors.
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

#include "ray/object_manager/push_manager.h"

#include <unistd.h>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

TEST(TestPushManager, TestSingleTransfer) {
  instrumented_io_context io_service;
  std::vector<int> results;
  results.resize(10);
  auto node_id = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm(5, io_service);
  pm.StartPush(
      node_id, obj_id, 10, 1, 1, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  ASSERT_EQ(pm.NumPushesInFlight(), 1);
  for (int i = 0; i < 10; i++) {
    pm.OnChunkComplete(node_id, {obj_id}, 1);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushesInFlight(), 0);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results[i], 1);
  }
}

TEST(TestPushManager, TestPushState) {
  // normal sending.
  {
    int64_t bytes_in_flight = 0;
    const int64_t max_bytes_in_flight = 2 * 1024 ^ 3;  // 2GB
    std::vector<int64_t> sent_chunks;
    auto obj_id = ObjectID::FromRandom();
    PushManager::PushState state{
        2, 2, 1, [&](int64_t chunk_id) { sent_chunks.push_back(chunk_id); }, obj_id};
    ASSERT_EQ(state.num_chunks, 2);
    ASSERT_EQ(state.next_chunk_id, 0);
    ASSERT_EQ(state.num_chunks_inflight, 0);
    ASSERT_EQ(state.num_chunks_to_send, 2);
    ASSERT_TRUE(state.SendOneChunk(bytes_in_flight, max_bytes_in_flight) > 0);
    ASSERT_FALSE(state.AllChunksComplete());
    ASSERT_EQ(state.num_chunks, 2);
    ASSERT_EQ(state.next_chunk_id, 1);
    ASSERT_EQ(state.num_chunks_inflight, 1);
    ASSERT_EQ(state.num_chunks_to_send, 1);
    std::vector<int64_t> expected_chunks{0};
    ASSERT_EQ(sent_chunks, expected_chunks);

    ASSERT_TRUE(state.SendOneChunk(bytes_in_flight, max_bytes_in_flight) > 0);
    ASSERT_EQ(state.num_chunks, 2);
    ASSERT_EQ(state.next_chunk_id, 0);
    ASSERT_EQ(state.num_chunks_inflight, 2);
    ASSERT_EQ(state.num_chunks_to_send, 0);
    std::vector<int64_t> expected_chunks1{0, 1};
    ASSERT_EQ(sent_chunks, expected_chunks1);
    ASSERT_FALSE(state.AllChunksComplete());

    ASSERT_FALSE(state.SendOneChunk(bytes_in_flight, max_bytes_in_flight) > 0);
    state.OnChunkComplete();
    ASSERT_EQ(state.num_chunks_inflight, 1);
    ASSERT_FALSE(state.AllChunksComplete());
    state.OnChunkComplete();
    ASSERT_EQ(state.num_chunks_inflight, 0);
    ASSERT_TRUE(state.AllChunksComplete());
  }

  // resend all chunks.
  {
    int64_t bytes_in_flight = 0;
    const int64_t max_bytes_in_flight = 2 * 1024 ^ 3;  // 2GB
    std::vector<int64_t> sent_chunks;
    auto obj_id = ObjectID::FromRandom();
    PushManager::PushState state{
        3, 2, 1, [&](int64_t chunk_id) { sent_chunks.push_back(chunk_id); }, obj_id};
    ASSERT_TRUE(state.SendOneChunk(bytes_in_flight, max_bytes_in_flight) > 0);
    ASSERT_FALSE(state.AllChunksComplete());
    ASSERT_EQ(state.num_chunks, 3);
    ASSERT_EQ(state.next_chunk_id, 1);
    ASSERT_EQ(state.num_chunks_inflight, 1);
    ASSERT_EQ(state.num_chunks_to_send, 2);
    std::vector<int64_t> expected_chunks{0};
    ASSERT_EQ(sent_chunks, expected_chunks);

    // resend chunks when 1 chunk is in flight.
    ASSERT_EQ(1, state.ResendAllChunks([&](int64_t chunk_id) {
      sent_chunks.push_back(chunk_id);
    }));
    ASSERT_EQ(state.num_chunks, 3);
    ASSERT_EQ(state.next_chunk_id, 1);
    ASSERT_EQ(state.num_chunks_inflight, 1);
    ASSERT_EQ(state.num_chunks_to_send, 3);

    for (auto i = 0; i < 3; i++) {
      ASSERT_TRUE(state.SendOneChunk(bytes_in_flight, max_bytes_in_flight) > 0);
      ASSERT_EQ(state.num_chunks, 3);
      ASSERT_EQ(state.next_chunk_id, (2 + i) % 3);
      ASSERT_EQ(state.num_chunks_inflight, 2 + i);
      ASSERT_EQ(state.num_chunks_to_send, 3 - i - 1);
    }
    std::vector<int64_t> expected_chunks1{0, 1, 2, 0};
    ASSERT_EQ(sent_chunks, expected_chunks1);

    ASSERT_FALSE(state.SendOneChunk(bytes_in_flight, max_bytes_in_flight) > 0);
    ASSERT_FALSE(state.AllChunksComplete());
    state.OnChunkComplete();
    state.OnChunkComplete();
    state.OnChunkComplete();
    ASSERT_FALSE(state.AllChunksComplete());
    state.OnChunkComplete();
    ASSERT_TRUE(state.AllChunksComplete());
  }
}

TEST(TestPushManager, TestRetryDuplicates) {
  instrumented_io_context io_service;
  std::vector<int> results;
  results.resize(10);
  auto node_id = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm(5, io_service);

  // First push request.
  pm.StartPush(
      node_id, obj_id, 10, 1, 1, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  ASSERT_EQ(pm.NumPushesInFlight(), 1);
  // Second push request will resent the full chunks.
  pm.StartPush(
      node_id, obj_id, 10, 1, 1, [&](int64_t chunk_id) { results[chunk_id] = 2; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 15);
  ASSERT_EQ(pm.NumPushesInFlight(), 1);
  // first 5 chunks will be sent by first push request.
  for (int i = 0; i < 5; i++) {
    pm.OnChunkComplete(node_id, {obj_id}, 1);
  }
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(results[i], 1);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 10);
  // we will resend all chunks by second push request.
  for (int i = 0; i < 10; i++) {
    pm.OnChunkComplete(node_id, {obj_id}, 1);
  }
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results[i], 2);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushesInFlight(), 0);
}

TEST(TestPushManager, TestMultipleTransfers) {
  instrumented_io_context io_service;
  std::vector<int> results1;
  results1.resize(10);
  std::vector<int> results2;
  results2.resize(10);
  auto node1 = NodeID::FromRandom();
  auto node2 = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  int num_active1 = 0;
  int num_active2 = 0;
  PushManager pm(5, io_service);
  pm.StartPush(node1, obj_id, 10, 1, 1, [&](int64_t chunk_id) {
    results1[chunk_id] = 1;
    num_active1++;
  });
  pm.StartPush(node2, obj_id, 10, 1, 1, [&](int64_t chunk_id) {
    results2[chunk_id] = 2;
    num_active2++;
  });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 20);
  ASSERT_EQ(pm.NumPushesInFlight(), 2);
  for (int i = 0; i < 20; i++) {
    if (num_active1 > 0) {
      pm.OnChunkComplete(node1, {obj_id}, 1);
      num_active1--;
    } else if (num_active2 > 0) {
      pm.OnChunkComplete(node2, {obj_id}, 1);
      num_active2--;
    }
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushesInFlight(), 0);
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results1[i], 1);
  }
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(results2[i], 2);
  }
}

TEST(TestPushManager, TestResendWholeObject) {
  std::vector<int> results;
  results.resize(5);
  auto node_id = NodeID::FromRandom();
  auto obj_id = ObjectID::FromRandom();
  PushManager pm(5);
  pm.StartPush(node_id, obj_id, 5, [&](int64_t chunk_id) { results[chunk_id] = 1; });
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 5);
  ASSERT_EQ(pm.NumPushesInFlight(), 1);
  pm.StartPush(node_id, obj_id, 5, [&](int64_t chunk_id) { results[chunk_id] = 2; });
  for (size_t i = 0; i < 5; i++) {
    pm.OnChunkComplete(node_id, obj_id);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 5);
  ASSERT_EQ(pm.NumChunksRemaining(), 5);
  ASSERT_EQ(pm.NumPushesInFlight(), 1);
  for (size_t i = 0; i < 5; i++) {
    ASSERT_EQ(results[i], 2);
  }
  for (size_t i = 0; i < 5; i++) {
    pm.OnChunkComplete(node_id, obj_id);
  }
  ASSERT_EQ(pm.NumChunksInFlight(), 0);
  ASSERT_EQ(pm.NumChunksRemaining(), 0);
  ASSERT_EQ(pm.NumPushesInFlight(), 0);
  for (size_t i = 0; i < 5; i++) {
    ASSERT_EQ(results[i], 2);
  }
}

TEST(TestPushManager, TestSingleObjectWithMultipleChunks) {
  instrumented_io_context io_service;
  auto loop_limits = RayConfig::instance().push_manager_loop_limits();
  auto chunk_num = loop_limits + 10;
  auto obj_id = ObjectID::FromRandom();
  const int64_t unlimit_bytes_in_flight = 1024 ^ 3;
  PushManager pm(unlimit_bytes_in_flight, io_service);
  auto node = NodeID::FromRandom();

  std::promise<void> promise;
  auto future = promise.get_future();
  io_service.post(
      [&]() {
        pm.StartPush(node, obj_id, chunk_num, 1, 1, [&](int64_t chunk_id) {
          // do nothing.
          return;
        });
        ASSERT_EQ(pm.NumChunksInFlight(), loop_limits);
        ASSERT_EQ(pm.NumChunksRemaining(), chunk_num);

        io_service.post(
            [&]() {
              for (int i = 0; i < chunk_num; i++) {
                pm.OnChunkComplete(node, {obj_id}, 1);
              }

              ASSERT_EQ(pm.NumChunksInFlight(), 0);
              ASSERT_EQ(pm.NumChunksRemaining(), 0);
              ASSERT_EQ(pm.NumPushesInFlight(), 0);
              promise.set_value();
            },
            "");
      },
      "");

  auto thread = std::thread([&]() {
    SetThreadName("TestSingleObjectWithMultipleChunks.main_thread");
    io_service.run();
  });

  future.wait();

  io_service.stop();
  thread.join();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
