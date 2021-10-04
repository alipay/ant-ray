import os
import logging

from typing import Optional, List, Dict, Any

import ray
from ray._private.runtime_env import RuntimeEnvContext

default_logger = logging.getLogger(__name__)


# NOTE(chenk008): it is moved from setup_worker. And it will be used
# to setup resource limit.
def parse_allocated_resource(allocated_instances_serialized_json):
    container_resource_args = []
    allocated_resource = json.loads(allocated_instances_serialized_json)
    if "CPU" in allocated_resource.keys():
        cpu_resource = allocated_resource["CPU"]
        if isinstance(cpu_resource, list):
            # cpuset: because we may split one cpu core into some pieces,
            # we need set cpuset.cpu_exclusive=0 and set cpuset-cpus
            cpu_ids = []
            cpu_shares = 0
            for idx, val in enumerate(cpu_resource):
                if val > 0:
                    cpu_ids.append(idx)
                    cpu_shares += val
            container_resource_args.append("--cpu-shares=" +
                                           str(int(cpu_shares / 10000 * 1024)))
            container_resource_args.append("--cpuset-cpus=" + ",".join(
                str(e) for e in cpu_ids))
        else:
            # cpushare
            container_resource_args.append(
                "--cpu-shares=" + str(int(cpu_resource / 10000 * 1024)))
    if "memory" in allocated_resource.keys():
        container_resource_args.append(
            "--memory=" + str(int(allocated_resource["memory"] / 10000)))
    return container_resource_args


def get_tmp_dir(remaining_args):
    for arg in remaining_args:
        if arg.startswith("--temp-dir="):
            return arg[11:]
    return None


class ContainerManager:
    def __init__(self, tmp_dir: str):
        self._tmp_dir = tmp_dir

    def setup(self,
              runtime_env: dict,
              context: RuntimeEnvContext,
              logger: Optional[logging.Logger] = default_logger):
        container_option = runtime_env.get("container")
        if not container_option and not container_option.get("image"):
            return

        container_driver = "podman"
        container_command = [
            container_driver, "run", "-v", self._tmp_dir + ":" + self._tmp_dir,
            "--cgroup-manager=cgroupfs", "--network=host", "--pid=host",
            "--ipc=host", "--env-host"
        ]
        container_command.append("--env")
        container_command.append("RAY_RAYLET_PID=" + os.getenv("RAY_RAYLET_PID"))
        if container_option.get("run_options"):
            container_command.extend(container_option.get("run_options"))
        # TODO(chenk008): add resource limit
        container_command.append("--entrypoint")
        container_command.append("bash")
        container_command.append(container_option.get("image"))
        container_command.append("-c")
        context.container_command_prefix = container_command
        logger.warning("start worker in container with prefix: {}".format(" ".join(container_command)))