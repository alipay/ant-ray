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

#include "ray/util/logging.h"

#include <chrono>
#include <cstdlib>
#include <iostream>

#include "absl/strings/str_format.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/util/filesystem.h"
#include "ray/util/fixed_string.h"
#include "ray/util/logging_new.h"

using namespace testing;

class ScopedTimer {
 public:
  ScopedTimer(const char *name)
      : m_name(name), m_beg(std::chrono::high_resolution_clock::now()) {}
  ~ScopedTimer() {
    auto end = std::chrono::high_resolution_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(end - m_beg);
    std::cout << m_name << " : " << dur.count() << " ns\n";
  }

 private:
  const char *m_name;
  std::chrono::time_point<std::chrono::high_resolution_clock> m_beg;
};

namespace ray {

int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

// This is not really test.
// This file just print some information using the logging macro.

void PrintLog() {
  RAY_LOG(DEBUG) << "This is the"
                 << " DEBUG"
                 << " message";
  RAY_LOG(INFO) << "This is the"
                << " INFO message";
  RAY_LOG(WARNING) << "This is the"
                   << " WARNING message";
  RAY_LOG(ERROR) << "This is the"
                 << " ERROR message";
  RAY_CHECK(true) << "This is a RAY_CHECK"
                  << " message but it won't show up";
  // The following 2 lines should not run since it will cause program failure.
  // RAY_LOG(FATAL) << "This is the FATAL message";
  // RAY_CHECK(false) << "This is a RAY_CHECK message but it won't show up";
}

TEST(PrintLogTest, LogTestWithoutInit) {
  // Without RayLog::StartRayLog, this should also work.
  PrintLog();
}

#if GTEST_HAS_STREAM_REDIRECTION
using testing::internal::CaptureStderr;
using testing::internal::GetCapturedStderr;

namespace {
void VerifyOnlyNthOccurenceLogged(bool fallback_to_debug) {
  const std::string kLogStr = "this is a test log";
  CaptureStderr();
  static int non_fallback_counter = 0;
  static int fallback_counter = 0;
  int &counter = fallback_to_debug ? fallback_counter : non_fallback_counter;
  for (int i = 0; i < 9; i++) {
    counter++;
    if (fallback_to_debug) {
      RAY_LOG_EVERY_N_OR_DEBUG(INFO, 3) << kLogStr;
    } else {
      RAY_LOG_EVERY_N(INFO, 3) << kLogStr;
    }
  }
  std::string output = GetCapturedStderr();
  for (int i = counter - 8; i <= counter; i++) {
    std::string expected_str = absl::StrFormat("[%d] this is a test log", i);
    if (i % 3 == 1) {
      EXPECT_THAT(output, HasSubstr(expected_str));
    } else {
      EXPECT_THAT(output, Not(HasSubstr(expected_str)));
    }
  }

  size_t occurrences = 0;
  std::string::size_type start = 0;

  while ((start = output.find(kLogStr, start)) != std::string::npos) {
    ++occurrences;
    start += kLogStr.length();
  }
  EXPECT_EQ(occurrences, 3);
}

void VerifyAllOccurenceLogged() {
  const std::string kLogStr = "this is a test log";
  CaptureStderr();
  for (int i = 0; i < 10; i++) {
    RAY_LOG_EVERY_N_OR_DEBUG(INFO, 3) << kLogStr;
  }
  std::string output = GetCapturedStderr();
  size_t occurrences = 0;
  std::string::size_type start = 0;
  while ((start = output.find("[0] this is a test log", start)) != std::string::npos) {
    ++occurrences;
    start += kLogStr.length();
  }
  EXPECT_EQ(occurrences, 10);
}

void VerifyNothingLogged(bool fallback_to_debug) {
  const std::string kLogStr = "this is a test log";
  CaptureStderr();
  for (int i = 0; i < 10; i++) {
    if (fallback_to_debug) {
      RAY_LOG_EVERY_N_OR_DEBUG(INFO, 3) << kLogStr;
    } else {
      RAY_LOG_EVERY_N(INFO, 3) << kLogStr;
    };
  }
  std::string output = GetCapturedStderr();

  size_t occurrences = 0;
  std::string::size_type start = 0;

  while ((start = output.find(kLogStr, start)) != std::string::npos) {
    ++occurrences;
    start += kLogStr.length();
  }
  EXPECT_EQ(occurrences, 0);
}
}  // namespace

TEST(PrintLogTest, TestRayLogEveryN) {
  RayLog::severity_threshold_ = RayLogLevel::INFO;
  VerifyOnlyNthOccurenceLogged(/*fallback_to_debug*/ false);

  RayLog::severity_threshold_ = RayLogLevel::DEBUG;
  VerifyOnlyNthOccurenceLogged(/*fallback_to_debug*/ false);

  RayLog::severity_threshold_ = RayLogLevel::WARNING;
  VerifyNothingLogged(/*fallback_to_debug*/ false);

  RayLog::severity_threshold_ = RayLogLevel::INFO;
}

TEST(PrintLogTest, TestRayLogEveryNOrDebug) {
  RayLog::severity_threshold_ = RayLogLevel::INFO;
  VerifyOnlyNthOccurenceLogged(/*fallback_to_debug*/ true);

  RayLog::severity_threshold_ = RayLogLevel::DEBUG;
  VerifyAllOccurenceLogged();

  RayLog::severity_threshold_ = RayLogLevel::WARNING;
  VerifyNothingLogged(/*fallback_to_debug*/ true);

  RayLog::severity_threshold_ = RayLogLevel::INFO;
}

TEST(PrintLogTest, TestRayLogEveryMs) {
  CaptureStderr();
  const std::string kLogStr = "this is a test log";
  auto start_time = std::chrono::steady_clock::now().time_since_epoch();
  size_t num_iterations = 0;
  while (std::chrono::steady_clock::now().time_since_epoch() - start_time <
         std::chrono::milliseconds(100)) {
    num_iterations++;
    RAY_LOG_EVERY_MS(INFO, 10) << kLogStr;
  }
  std::string output = GetCapturedStderr();
  size_t occurrences = 0;
  std::string::size_type start = 0;

  while ((start = output.find(kLogStr, start)) != std::string::npos) {
    ++occurrences;
    start += kLogStr.length();
  }
  EXPECT_LT(occurrences, num_iterations);
  EXPECT_GT(occurrences, 5);
  EXPECT_LT(occurrences, 15);
}

#endif /* GTEST_HAS_STREAM_REDIRECTION */

TEST(PrintLogTest, LogTestWithInit) {
  // Test empty app name.
  RayLog::StartRayLog("", RayLogLevel::DEBUG, ray::GetUserTempDir() + ray::GetDirSep());
  PrintLog();
  RayLog::ShutDownRayLog();
}

TEST(LogPerfTest, NewLogTest) {
  ray_test::RayLog::StartRayLog("aa", ray_test::RayLogLevelNew::DEBUG,
                                ray::GetUserTempDir() + ray::GetDirSep());
  RAY_LOG_NEW(INFO) << "This is the"
                    << " INFO_NEW"
                    << " message";

  ray::RayLog::StartRayLog("bb", ray::RayLogLevel::DEBUG,
                           ray::GetUserTempDir() + ray::GetDirSep());
  RAY_LOG(INFO) << "This is the"
                << " INFO_NEW"
                << " message";

  const int rounds = 100000;
  {
    ScopedTimer timer("old debug log");
    for (int i = 0; i < rounds; ++i) {
      RAY_LOG(DEBUG) << "This is the "
                     << "RAY_DEBUG message";
    }
  }

  {
    ScopedTimer timer("new debug log");
    for (int i = 0; i < rounds; ++i) {
      RAY_LOG_NEW(DEBUG) << "This is the "
                         << "RAY_DEBUG message";
    }
  }

  {
    ScopedTimer timer("old info log");
    for (int i = 0; i < rounds; ++i) {
      RAY_LOG(INFO) << "This is the "
                    << "RAY_INFO message";
    }
  }

  {
    ScopedTimer timer("new info log");
    for (int i = 0; i < rounds; ++i) {
      RAY_LOG_NEW(INFO) << "This is the "
                        << "RAY_INFO message";
    }
  }
}

// This test will output large amount of logs to stderr, should be disabled in travis.
TEST(LogPerfTest, PerfTest) {
  RayLog::StartRayLog("/fake/path/to/appdire/LogPerfTest", RayLogLevel::ERROR,
                      ray::GetUserTempDir() + ray::GetDirSep());
  int rounds = 100000;

  int64_t start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    RAY_LOG(DEBUG) << "This is the "
                   << "RAY_DEBUG message";
  }
  int64_t elapsed = current_time_ms() - start_time;
  std::cout << "Testing DEBUG log for " << rounds << " rounds takes " << elapsed << " ms."
            << std::endl;

  start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    RAY_LOG(ERROR) << "This is the "
                   << "RAY_ERROR message";
  }
  elapsed = current_time_ms() - start_time;
  std::cout << "Testing RAY_ERROR log for " << rounds << " rounds takes " << elapsed
            << " ms." << std::endl;

  start_time = current_time_ms();
  for (int i = 0; i < rounds; ++i) {
    RAY_CHECK(i >= 0) << "This is a RAY_CHECK "
                      << "message but it won't show up";
  }
  elapsed = current_time_ms() - start_time;
  std::cout << "Testing RAY_CHECK(true) for " << rounds << " rounds takes " << elapsed
            << " ms." << std::endl;
  RayLog::ShutDownRayLog();
}

std::string TestFunctionLevel0() {
  std::string call_trace = GetCallTrace();
  RAY_LOG(INFO) << "TestFunctionLevel0\n" << call_trace;
  return call_trace;
}

std::string TestFunctionLevel1() {
  RAY_LOG(INFO) << "TestFunctionLevel1:";
  return TestFunctionLevel0();
}

std::string TestFunctionLevel2() {
  RAY_LOG(INFO) << "TestFunctionLevel2:";
  return TestFunctionLevel1();
}

#ifndef _WIN32
TEST(PrintLogTest, CallstackTraceTest) {
  auto ret0 = TestFunctionLevel0();
  EXPECT_TRUE(ret0.find("TestFunctionLevel0") != std::string::npos);
  auto ret1 = TestFunctionLevel1();
  EXPECT_TRUE(ret1.find("TestFunctionLevel1") != std::string::npos);
  auto ret2 = TestFunctionLevel2();
  EXPECT_TRUE(ret2.find("TestFunctionLevel2") != std::string::npos);
}
#endif

TEST(LogTest, FixedStringTest) {
  constexpr auto s = make_fixed_string("hello");
  static_assert(s.size() == 5);
  constexpr auto s1 = s + " world";
  std::cout << s1.size() << '\n';
  static_assert(s1.size() == 11);

  fixed_string<5> str;
  std::cout << str.size() << '\n';

  constexpr auto s3 = s.substr<3>();
  std::cout << s3.size() << " " << s3.data() << " " << s.data() << '\n';
  static_assert(s3 == "lo");

  constexpr auto s4 = make_fixed_string("/tmp/log/main.cpp");
  constexpr auto pos = s4.rfind('/');
  constexpr auto s5 = s4.substr<pos + 1>();

  static_assert(s5 == "main.cpp");
  constexpr auto s6 = s5 + ":" + ":";
  static_assert(s6 == "main.cpp::");
}
/// Catch abort signal handler for testing RAY_CHECK.
/// We'd better to run the following test case manually since process
/// will terminated if abort signal raising.
/*
bool get_abort_signal = false;
void signal_handler(int signum) {
  RAY_LOG(WARNING) << "Interrupt signal (" << signum << ") received.";
  get_abort_signal = signum == SIGABRT;
  exit(0);
}

TEST(PrintLogTest, RayCheckAbortTest) {
  get_abort_signal = false;
  // signal(SIGABRT, signal_handler);
  ray::RayLog::InstallFailureSignalHandler();
  RAY_CHECK(0) << "Check for aborting";
  sleep(1);
  EXPECT_TRUE(get_abort_signal);
}
*/

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
