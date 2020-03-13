#include "gcs_detector.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {
namespace gcs {

GcsDetector::GcsDetector(boost::asio::io_service &io_service,
                         std::shared_ptr<gcs::RedisGcsClient> gcs_client)
    : gcs_client_(std::move(gcs_client)), detect_timer_(io_service) {
  Start();
}

void GcsDetector::Start() {
  RAY_LOG(INFO) << "Starting gcs node manager.";
  Tick();
}

void GcsDetector::DetectGcs() {}

/// A periodic timer that checks for timed out clients.
void GcsDetector::Tick() {
  DetectGcs();
  ScheduleTick();
}

void GcsDetector::ScheduleTick() {
  auto detect_period = boost::posix_time::milliseconds(
      RayConfig::instance().gcs_detect_timeout_milliseconds());
  detect_timer_.expires_from_now(detect_period);
  detect_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      // `operation_canceled` is set when `heartbeat_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Checking gcs detect failed with error: " << error.message();
    Tick();
  });
}

}  // namespace gcs
}  // namespace ray