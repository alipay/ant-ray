from ray.ray_constants import env_integer
from ray.core.generated import event_pb2

EVENT_MODULE_ENVIRONMENT_KEY = "RAY_DASHBOARD_MODULE_EVENT"
LOG_ERROR_EVENT_STRING_LENGTH_LIMIT = 1000
RETRY_CONNECT_TO_DASHBOARD_INTERVAL_SECONDS = 2
# Monitor events
SCAN_EVENT_DIR_INTERVAL_SECONDS = env_integer(
    "SCAN_EVENT_DIR_INTERVAL_SECONDS", 2)
SCAN_EVENT_START_OFFSET_SECONDS = -30 * 60
CONCURRENT_READ_LIMIT = 50
EVENT_READ_LINE_COUNT_LIMIT = 200
EVENT_READ_LINE_LENGTH_LIMIT = env_integer("EVENT_READ_LINE_LENGTH_LIMIT",
                                           2 * 1024 * 1024)  # 2MB
# Report events
EVENT_AGENT_REPORT_INTERVAL_SECONDS = 0.1
EVENT_AGENT_RETRY_TIMES = 10
EVENT_AGENT_CACHE_SIZE = 10240
# Event sources
EVENT_HEAD_MONITOR_SOURCE_TYPES = [
    event_pb2.Event.SourceType.Name(event_pb2.Event.GCS)
]
EVENT_AGENT_MONITOR_SOURCE_TYPES = list(
    set(event_pb2.Event.SourceType.keys()) -
    set(EVENT_HEAD_MONITOR_SOURCE_TYPES))
EVENT_SOURCE_ALL = event_pb2.Event.SourceType.keys()
