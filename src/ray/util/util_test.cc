#include <chrono>
#include <cstdlib>
#include <iostream>

#include "gtest/gtest.h"
#include "ray/util/logging.h"
#include "ray/util/signal_handler.h"
#include <signal.h>

// This test just print some call stack information.
namespace ray {

void Sleep() {
  usleep(100000);
}

void TestSendSignal(const std::string &test_name, int signal) {
  pid_t pid;
  pid=fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    while (true) {
      int n = 1000;
      while (n--);
    }
  } else {
    Sleep();
    std::cout << test_name << ": kill pid " << pid << " with return value=" << kill(pid, signal) << std::endl;
    Sleep();
  }
}

TEST(SignalTest, SendTermSignal_Unset_Test) {
  ray::SignalHandler::InstallSingalHandler("util_test", false);
  // This should not print call stack message.
  TestSendSignal("SendTermSignal_Unset_Test", SIGTERM);
  ray::SignalHandler::UninstallSingalHandler();
}

TEST(SignalTest, SendTermSignalTest) {
  ray::SignalHandler::InstallSingalHandler("util_test", true);
  TestSendSignal("SendTermSignalTest", SIGTERM);
  ray::SignalHandler::UninstallSingalHandler();
}

TEST(SignalTest, SendIntSignalTest) {
  ray::SignalHandler::InstallSingalHandler("util_test", false);
  TestSendSignal("SendIntSignalTest", SIGINT);
  ray::SignalHandler::UninstallSingalHandler();
}

TEST(SignalTest, SIGSEGV_Test) {
  ray::SignalHandler::InstallSingalHandler("util_test", true);
  pid_t pid;
  pid=fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    int *pointer = (int *)0x1237896;
    *pointer = 100;
  } else {
    Sleep();
    std::cout << "SIGSEGV_Test: kill pid " << pid << " with return value=" << kill(pid, SIGKILL) << std::endl;
    Sleep();
  }
  ray::SignalHandler::UninstallSingalHandler();
}

TEST(SignalTest, SIGILL_Test) {
  ray::SignalHandler::InstallSingalHandler("util_test", false);
  pid_t pid;
  pid=fork();
  ASSERT_TRUE(pid >= 0);
  if (pid == 0) {
    int *pointer = nullptr;
    *pointer = 100;
  } else {
    Sleep();
    std::cout << "SIGILL_Test: kill pid " << pid << " with return value=" << kill(pid, SIGKILL) << std::endl;
    Sleep();
  }
  ray::SignalHandler::UninstallSingalHandler();
}

}  // namespace ray

int main(int argc, char **argv) {
  ray::RayLog::StartRayLog("", RAY_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  int failed = RUN_ALL_TESTS();
  ray::RayLog::ShutDownRayLog();
  return failed;
}
