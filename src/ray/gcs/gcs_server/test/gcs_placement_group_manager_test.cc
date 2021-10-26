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

#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

using ::testing::_;
using StatusCallback = std::function<void(Status status)>;

class MockPlacementGroupScheduler : public gcs::GcsPlacementGroupSchedulerInterface {
 public:
  MockPlacementGroupScheduler() = default;

  void ScheduleUnplacedBundles(
      std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>, bool)> failure_handler,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)> success_handler)
      override {
    absl::MutexLock lock(&mutex_);
    placement_groups_.push_back(placement_group);
  }

  MOCK_METHOD1(DestroyPlacementGroupBundleResourcesIfExists,
               void(const PlacementGroupID &placement_group_id));

  MOCK_METHOD1(MarkScheduleCancelled, void(const PlacementGroupID &placement_group_id));

  MOCK_METHOD1(
      ReleaseUnusedBundles,
      void(const std::unordered_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles));

  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const NodeID &node_id) override {
    absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> bundles;
    bundles[group_on_dead_node_] = bundles_on_dead_node_;
    return bundles;
  }

  int GetPlacementGroupCount() {
    absl::MutexLock lock(&mutex_);
    return placement_groups_.size();
  }

  PlacementGroupID group_on_dead_node_;
  std::vector<int64_t> bundles_on_dead_node_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> placement_groups_;
  absl::Mutex mutex_;
};

class GcsPlacementGroupManagerTest : public ::testing::Test {
 public:
  GcsPlacementGroupManagerTest()
      : mock_placement_group_scheduler_(new MockPlacementGroupScheduler()) {
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_resource_manager_ =
        std::make_shared<gcs::GcsResourceManager>(io_service_, nullptr, nullptr, true);
    gcs_placement_group_manager_.reset(new gcs::GcsPlacementGroupManager(
        io_service_, mock_placement_group_scheduler_, gcs_table_storage_,
        *gcs_resource_manager_, gcs_pub_sub_,
        [this](const JobID &job_id) { return job_namespace_table_[job_id]; }));
    for (int i = 1; i <= 10; i++) {
      auto job_id = JobID::FromInt(i);
      job_namespace_table_[job_id] = "";
    }
  }

  void SetUp() override {
    // mock_placement_group_scheduler_.reset(new MockPlacementGroupScheduler());
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));
  }

  void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  // Make placement group registration sync.
  std::shared_ptr<gcs::GcsPlacementGroup> RegisterPlacementGroup(const ray::rpc::CreatePlacementGroupRequest &request,
                              StatusCallback callback) {
    std::promise<void> promise;
    JobID job_id = JobID::FromBinary(request.placement_group_spec().creator_job_id());
    std::string ray_namespace = job_namespace_table_[job_id];
    const auto &placement_group = std::make_shared<gcs::GcsPlacementGroup>(request, ray_namespace);
    gcs_placement_group_manager_->RegisterPlacementGroup(
        placement_group,
        [&callback, &promise](Status status) {
          RAY_CHECK_OK(status);
          callback(status);
          promise.set_value();
        });
    promise.get_future().get();
    return placement_group;
  }

  // We need this to ensure that `MarkSchedulingDone` and `SchedulePendingPlacementGroups`
  // was already invoked when we have invoked `OnPlacementGroupCreationSuccess`.
  void OnPlacementGroupCreationSuccess(
      const std::shared_ptr<gcs::GcsPlacementGroup> &placement_group) {
    std::promise<void> promise;
    gcs_placement_group_manager_->WaitPlacementGroup(
        placement_group->GetPlacementGroupID(), [&promise](Status status) {
          RAY_CHECK_OK(status);
          promise.set_value();
        });
    gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
    promise.get_future().get();
  }

  void AddBundlesForPlacementGroup(
      const PlacementGroupID &placement_group_id,
      const ray::rpc::AddPlacementGroupBundlesRequest &request) {
    std::promise<void> promise;
    gcs_placement_group_manager_->AddBundlesForPlacementGroup(
        placement_group_id, request, [&promise](const Status &status) {
          RAY_CHECK(status.ok());
          promise.set_value();
        });
    promise.get_future().get();
  }

  void WaitForExpectedPgCount(int expected_count) {
    auto condition = [this, expected_count]() {
      return mock_placement_group_scheduler_->GetPlacementGroupCount() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
  }

  ExponentialBackOff GetExpBackOff() { return ExponentialBackOff(0, 1); }

  std::shared_ptr<MockPlacementGroupScheduler> mock_placement_group_scheduler_;
  std::unique_ptr<gcs::GcsPlacementGroupManager> gcs_placement_group_manager_;
  std::unordered_map<JobID, std::string> job_namespace_table_;

 private:
  std::unique_ptr<std::thread> thread_io_service_;
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
};

TEST_F(GcsPlacementGroupManagerTest, TestPlacementGroupBundleCache) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  ASSERT_TRUE(placement_group->cached_bundle_specs_.empty());
  // Fill the cache and verify it.
  const auto &bundle_specs = placement_group->GetBundles();
  ASSERT_EQ(placement_group->cached_bundle_specs_, bundle_specs);
  ASSERT_FALSE(placement_group->cached_bundle_specs_.empty());
  // Invalidate the cache and verify it.
  RAY_UNUSED(placement_group->GetMutableBundle(0));
  ASSERT_TRUE(placement_group->cached_bundle_specs_.empty());
}

TEST_F(GcsPlacementGroupManagerTest, TestBasic) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingFailed) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group,
                                                               GetExpBackOff(), true);
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Check that the placement_group is in state `CREATED`.
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestGetPlacementGroupIDByName) {
  auto request = Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(
      gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
      PlacementGroupID::FromBinary(request.placement_group_spec().placement_group_id()));
}

TEST_F(GcsPlacementGroupManagerTest, TestRemoveNamedPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // Remove the named placement group.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group->GetPlacementGroupID(),
      [](const Status &status) { ASSERT_TRUE(status.ok()); });
  ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
            PlacementGroupID::Nil());
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeAdd) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // If the creation of placement group fails, it will be rescheduled after a short time.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group,
                                                               GetExpBackOff(), true);
  WaitForExpectedPgCount(1);
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingPendingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group,
                                                               GetExpBackOff(), true);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingLeasingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);

  // Placement group is in leasing state.
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group,
                                                               GetExpBackOff(), true);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingCreatedPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // We have ensured that this operation is synchronized.
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeDead) {
  auto request1 = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request1, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  auto request2 = Mocker::GenCreatePlacementGroupRequest();
  RegisterPlacementGroup(request2, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 2);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  placement_group->GetMutableBundle(0)->set_node_id(NodeID::FromRandom().Binary());
  placement_group->GetMutableBundle(1)->set_node_id(NodeID::FromRandom().Binary());
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  // If a node dies, we will set the bundles above it to be unplaced and reschedule the
  // placement group. The placement group state is set to `RESCHEDULING` and will be
  // scheduled first.
  mock_placement_group_scheduler_->group_on_dead_node_ =
      placement_group->GetPlacementGroupID();
  mock_placement_group_scheduler_->bundles_on_dead_node_.push_back(0);
  gcs_placement_group_manager_->OnNodeDead(NodeID::FromRandom());

  // Trigger scheduling `RESCHEDULING` placement group.
  auto finished_group = std::make_shared<gcs::GcsPlacementGroup>(
      placement_group->GetPlacementGroupTableData());
  OnPlacementGroupCreationSuccess(finished_group);
  ASSERT_EQ(finished_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  WaitForExpectedPgCount(1);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
  const auto &bundles =
      mock_placement_group_scheduler_->placement_groups_[0]->GetBundles();
  EXPECT_TRUE(NodeID::FromBinary(bundles[0]->GetMessage().node_id()).IsNil());
  EXPECT_FALSE(NodeID::FromBinary(bundles[1]->GetMessage().node_id()).IsNil());

  // If `RESCHEDULING` placement group fails to create, we will schedule it again first.
  placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group,
                                                               GetExpBackOff(), true);
  WaitForExpectedPgCount(1);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenActorDeadAndJobDead) {
  // Test the scenario where actor dead -> job dead.
  const auto job_id = JobID::FromInt(1);
  const auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ actor_id);
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // When both job and actor is dead, placement group should be destroyed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  // Placement group shouldn't be cleaned when only an actor is killed.
  // When both job and actor is dead, placement group should be destroyed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenActorAndJobDead) {
  // Test the scenario where job dead -> actor dead.
  const auto job_id = JobID::FromInt(1);
  const auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ actor_id);
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  // Placement group shouldn't be cleaned when only an actor is killed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenOnlyJobDead) {
  // Test placement group is cleaned when both actor & job are dead.
  const auto job_id = JobID::FromInt(1);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ ActorID::Nil());
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
}

TEST_F(GcsPlacementGroupManagerTest,
       TestAutomaticCleanupDoNothingWhenDifferentJobIsDead) {
  // Test placement group is cleaned when both actor & job are dead.
  const auto job_id = JobID::FromInt(1);
  const auto different_job_id = JobID::FromInt(3);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ ActorID::Nil());
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // This shouldn't have been called.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingCanceledWhenPgIsInfeasible) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Mark it non-retryable.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group,
                                                               GetExpBackOff(), false);

  // Schedule twice to make sure it will not be scheduled afterward.
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);

  // Add a node and make sure it will reschedule the infeasible placement group.
  const auto &node_id = NodeID::FromRandom();
  gcs_placement_group_manager_->OnNodeAdd(node_id);

  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  mock_placement_group_scheduler_->placement_groups_.clear();

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestRayNamespace) {
  auto request1 = Mocker::GenCreatePlacementGroupRequest("test_name");
  job_namespace_table_[JobID::FromInt(11)] = "another_namespace";
  auto request2 = Mocker::GenCreatePlacementGroupRequest(
      "test_name", rpc::PlacementStrategy::SPREAD, 2, 1.0, JobID::FromInt(11));
  auto request3 = Mocker::GenCreatePlacementGroupRequest("test_name");
  {  // Create a placement group in the empty namespace.
    std::atomic<int> registered_placement_group_count(0);
    RegisterPlacementGroup(request1, [&registered_placement_group_count](Status status) {
      ++registered_placement_group_count;
    });

    ASSERT_EQ(registered_placement_group_count, 1);
    WaitForExpectedPgCount(1);
    auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
    mock_placement_group_scheduler_->placement_groups_.pop_back();

    OnPlacementGroupCreationSuccess(placement_group);
    ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
  {  // Create a placement group in the empty namespace.
    job_namespace_table_[JobID::FromInt(11)] = "another_namespace";
    std::atomic<int> registered_placement_group_count(0);
    RegisterPlacementGroup(request2, [&registered_placement_group_count](Status status) {
      ++registered_placement_group_count;
    });

    ASSERT_EQ(registered_placement_group_count, 1);
    WaitForExpectedPgCount(1);
    auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
    mock_placement_group_scheduler_->placement_groups_.pop_back();

    OnPlacementGroupCreationSuccess(placement_group);
    ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName(
                  "test_name", "another_namespace"),
              PlacementGroupID::FromBinary(
                  request2.placement_group_spec().placement_group_id()));
    ASSERT_NE(gcs_placement_group_manager_->GetPlacementGroupIDByName(
                  "test_name", "another_namespace"),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
  {  // Placement groups with the same namespace, different jobs should still collide.
    std::promise<void> promise;
    gcs_placement_group_manager_->RegisterPlacementGroup(
        std::make_shared<gcs::GcsPlacementGroup>(request3, ""),
        [&promise](Status status) {
          ASSERT_FALSE(status.ok());
          promise.set_value();
        });
    promise.get_future().get();

    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
}

TEST_F(GcsPlacementGroupManagerTest, TestAddBundlesWhenNotScheduling) {
  // 1.Test add bundles when state is `Created`.
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  auto add_bundles_request =
      Mocker::GenAddPlacementGroupBundlesRequest(placement_group->GetPlacementGroupID());
  AddBundlesForPlacementGroup(placement_group->GetPlacementGroupID(),
                              add_bundles_request);
  WaitForExpectedPgCount(1);
  placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::UPDATING);
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  // 2.Test add bundles when state is `Pending` or `Updating` but not scheduling.
  auto request2 = Mocker::GenCreatePlacementGroupRequest();
  RegisterPlacementGroup(request2,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });
  auto request3 = Mocker::GenCreatePlacementGroupRequest();
  const auto &pending_but_not_scheduling_group = RegisterPlacementGroup(request3,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });
  ASSERT_EQ(registered_placement_group_count, 3);
  WaitForExpectedPgCount(1);
  auto scheduling_placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_NE(scheduling_placement_group->GetPlacementGroupID(), pending_but_not_scheduling_group->GetPlacementGroupID());
  
  add_bundles_request =
      Mocker::GenAddPlacementGroupBundlesRequest(pending_but_not_scheduling_group->GetPlacementGroupID());
  AddBundlesForPlacementGroup(pending_but_not_scheduling_group->GetPlacementGroupID(), add_bundles_request);
  ASSERT_EQ(pending_but_not_scheduling_group->GetState(), rpc::PlacementGroupTableData::UPDATING);
  // Add twice when state is `UPDATING`.
  AddBundlesForPlacementGroup(pending_but_not_scheduling_group->GetPlacementGroupID(), add_bundles_request);
  AddBundlesForPlacementGroup(pending_but_not_scheduling_group->GetPlacementGroupID(), add_bundles_request);
  ASSERT_EQ(pending_but_not_scheduling_group->GetState(), rpc::PlacementGroupTableData::UPDATING);
  ASSERT_FALSE(pending_but_not_scheduling_group->IsNeedReschedule());

  OnPlacementGroupCreationSuccess(scheduling_placement_group);
  ASSERT_EQ(scheduling_placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  OnPlacementGroupCreationSuccess(pending_but_not_scheduling_group);
  ASSERT_EQ(pending_but_not_scheduling_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // Check the bundle size.
  ASSERT_EQ(pending_but_not_scheduling_group->GetBundles().size(), 8);
}

TEST_F(GcsPlacementGroupManagerTest, TestAddBundlesWhenScheduling) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  // So the placement group is in scheduling right now.
  auto add_bundles_request =
      Mocker::GenAddPlacementGroupBundlesRequest(placement_group->GetPlacementGroupID());
  // Add twice for test!
  AddBundlesForPlacementGroup(placement_group->GetPlacementGroupID(), add_bundles_request);
  AddBundlesForPlacementGroup(placement_group->GetPlacementGroupID(), add_bundles_request);
  WaitForExpectedPgCount(0);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::UPDATING);
  // It should be marked as 'need to be reschedule' now;
  ASSERT_TRUE(placement_group->IsNeedReschedule());
  // Check bundle size.
  ASSERT_EQ(placement_group->GetBundles().size(), 6);
  // Invoke the successful callback, it should be rescheduled immediately instead of transforming into `UPDATING` state!
  gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::UPDATING);
  // WaitForExpectedPgCount(1);
  // mock_placement_group_scheduler_->placement_groups_.clear();
  // AddBundlesForPlacementGroup(placement_group->GetPlacementGroupID(), add_bundles_request);
  // WaitForExpectedPgCount(0);
  // ASSERT_EQ(placement_group->GetBundles().size(), 8);
  // // Re-invoke the successful callback, it will re-execute the schedule.
  // gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
  // ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::UPDATING);
  // WaitForExpectedPgCount(1);
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "*TestAddBundlesWhenScheduling*";
  return RUN_ALL_TESTS();
}
