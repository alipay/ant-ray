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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gtest/gtest.h"
#include "ray/raylet/scheduling/cluster_resource_data.h"

namespace ray {

class ResourceRequestTest : public ::testing::Test {};

TEST_F(ResourceRequestTest, TestBasic) {
  auto cpu_id = ResourceID::CPU();
  auto gpu_id = ResourceID::GPU();
  auto custom_id1 = ResourceID("custom1");
  auto custom_id2 = ResourceID("custom2");

  absl::flat_hash_map<ResourceID, FixedPoint> resource_map(
      {{cpu_id, 1}, {custom_id1, 2}});

  ResourceRequest resource_request(resource_map);

  // Test Has
  ASSERT_TRUE(resource_request.Has(cpu_id));
  ASSERT_TRUE(resource_request.Has(custom_id1));
  ASSERT_FALSE(resource_request.Has(gpu_id));
  ASSERT_FALSE(resource_request.Has(custom_id2));

  // Test Get and GetOrZero
  ASSERT_EQ(resource_request.Get(cpu_id), 1);
  ASSERT_EQ(resource_request.Get(custom_id1), 2);
  ASSERT_EQ(resource_request.GetOrZero(gpu_id), 0);
  ASSERT_EQ(resource_request.GetOrZero(custom_id2), 0);

  // Test Size and IsEmpty
  ASSERT_EQ(resource_request.Size(), 2);
  ASSERT_FALSE(resource_request.IsEmpty());

  // Test ResourceIds and ToMap
  ASSERT_EQ(resource_request.ResourceIds(),
            absl::flat_hash_set<ResourceID>({cpu_id, custom_id1}));
  ASSERT_EQ(resource_request.ToMap(), resource_map);

  // Test Set
  resource_request.Set(gpu_id, 1);
  resource_request.Set(custom_id2, 2);
  ASSERT_TRUE(resource_request.Has(gpu_id));
  ASSERT_TRUE(resource_request.Has(custom_id2));
  ASSERT_EQ(resource_request.Get(gpu_id), 1);
  ASSERT_EQ(resource_request.Get(custom_id2), 2);
  // Set 0 will remove the resource
  resource_request.Set(cpu_id, 0);
  resource_request.Set(custom_id1, 0);
  ASSERT_FALSE(resource_request.Has(cpu_id));
  ASSERT_FALSE(resource_request.Has(custom_id1));

  // Test Clear
  resource_request.Clear();
  ASSERT_EQ(resource_request.Size(), 0);
  ASSERT_TRUE(resource_request.IsEmpty());
}

TEST_F(ResourceRequestTest, TestOperators) {
  auto cpu_id = ResourceID::CPU();
  auto custom_id1 = ResourceID("custom1");

  ResourceRequest r1;
  r1.Set(cpu_id, 1);
  r1.Set(custom_id1, 2);

  // Test operator=, operator==, and operator!=
  ResourceRequest r2 = r1;
  ASSERT_TRUE(r1 == r2);
  r2.Set(cpu_id, 2);
  ASSERT_TRUE(r1 != r2);

  // Test operator<= and operator>=
  // r1 = {CPU: 1, custom1: 2}, r2 = {CPU: 2, custom1:2}
  ASSERT_TRUE(r1 <= r2);
  ASSERT_TRUE(r2 >= r1);
  r2.Set(custom_id1, 1);
  ASSERT_FALSE(r1 <= r2);

  // Test operator+ and operator+=
  // r1 = {CPU: 1, custom1: 2}, r3 = {CPU: 2, custom1: 4}
  ResourceRequest r3 = r1 + r1;
  absl::flat_hash_map<ResourceID, FixedPoint> expected = {{cpu_id, 2}, {custom_id1, 4}};
  ASSERT_EQ(r3.ToMap(), expected);
  r3 += r1;
  expected = {{cpu_id, 3}, {custom_id1, 6}};
  ASSERT_EQ(r3.ToMap(), expected);

  // Test operator- and operator-=
  // r1 = {CPU: 1, custom1: 2}, r3 = {CPU: 3, custom1: 6}
  ResourceRequest r4 = r3 - r1;
  expected = {{cpu_id, 2}, {custom_id1, 4}};
  ASSERT_EQ(r4.ToMap(), expected);

  r1 -= r4;
  expected = {{cpu_id, -1}, {custom_id1, -2}};
  ASSERT_EQ(r1.ToMap(), expected);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
