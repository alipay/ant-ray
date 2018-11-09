#ifndef RAY_METRICS_GROUP_METRICS_GROUP_INTERFACE_H
#define RAY_METRICS_GROUP_METRICS_GROUP_INTERFACE_H

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <boost/noncopyable.hpp>

#include "ray/metrics/registry/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class MetricsGroupInterface : public std::enable_shared_from_this<MetricsGroupInterface>,
  boost::noncopyable {
 public:
  virtual ~MetricsGroupInterface();

  /// update counter by short name
  virtual void UpdateCounter(const std::string &short_name, int64_t value) = 0;

  /// update gauge by short name
  virtual void UpdateGauge(const std::string &short_name, int64_t value) = 0;

  /// update histogram by short name
  virtual void UpdateHistogram(const std::string &short_name,
                               int64_t value,
                               int64_t min_value,
                               int64_t max_value) = 0;

  virtual const std::string &GetGroupName() const {
    return group_name_;
  }

  virtual const std::string &GetDomain() const {
    return domain_;
  }

  virtual void SetRegistry(MetricsRegistryInterface *registry) {
    // TODO(micafan) CHECK not null
    registry_ = registry;
  }

 protected:
  MetricsGroupInterface(
    const std::string& domain,
    const std::string& group_name,
    const std::map<std::string, std::string> &tag_map = {});

  /// The domain of current group
  std::string domain_;
  /// The name of current group
  std::string group_name_;

  /// Tags of current group
  Tags *tags_{nullptr};

  MetricsRegistryInterface *registry_{nullptr};
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_GROUP_METRICS_GROUP_INTERFACE_H
