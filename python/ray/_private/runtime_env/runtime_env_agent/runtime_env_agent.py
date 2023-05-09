import asyncio
import json
import logging
import os
import time
import traceback
import argparse
import sys
import signal
import psutil

try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc

from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, List, Set, Tuple
from ray._private.ray_constants import (
    DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS,
)
import ray._private.ray_constants as ray_constants
from ray._private.runtime_env.runtime_env_agent import runtime_env_consts
from ray._private.ray_logging import setup_component_logger
from ray._private.gcs_utils import GcsAioClient
from ray._private.runtime_env.conda import CondaPlugin
from ray._private.runtime_env.container import ContainerManager
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.java_jars import JavaJarsPlugin
from ray._private.runtime_env.pip import PipPlugin
from ray._private.runtime_env.plugin import (
    RuntimeEnvPlugin,
    create_for_plugin_if_needed,
)
from ray._private.utils import get_or_create_event_loop, init_grpc_channel
from ray._private.tls_utils import add_port_to_grpc_server
from ray._private.runtime_env.plugin import RuntimeEnvPluginManager
from ray._private.runtime_env.py_modules import PyModulesPlugin
from ray._private.runtime_env.working_dir import WorkingDirPlugin
from ray.core.generated import (
    runtime_env_agent_pb2,
    runtime_env_agent_pb2_grpc,
    runtime_env_agent_manager_pb2,
    runtime_env_agent_manager_pb2_grpc,
)
from ray.core.generated.runtime_env_agent_manager_pb2 import (
    RUNTIME_ENV_AGENT_RPC_STATUS_OK,
    RUNTIME_ENV_AGENT_RPC_STATUS_FAILED,
)
from ray.core.generated.runtime_env_common_pb2 import (
    RuntimeEnvState as ProtoRuntimeEnvState,
)
from ray.runtime_env import RuntimeEnv, RuntimeEnvConfig

default_logger = logging.getLogger(__name__)
try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future


# TODO(edoakes): this is used for unit tests. We should replace it with a
# better pluggability mechanism once available.
SLEEP_FOR_TESTING_S = os.environ.get("RAY_RUNTIME_ENV_SLEEP_FOR_TESTING_S")
_PARENT_DEATH_THREASHOLD = 5


@dataclass
class CreatedEnvResult:
    # Whether or not the env was installed correctly.
    success: bool
    # If success is True, will be a serialized RuntimeEnvContext
    # If success is False, will be an error message.
    result: str
    # The time to create a runtime env in ms.
    creation_time_ms: int


# e.g., "working_dir"
UriType = str


class ReferenceTable:
    """
    The URI reference table which is used for GC.
    When the reference count is decreased to zero,
    the URI should be removed from this table and
    added to cache if needed.
    """

    def __init__(
        self,
        uris_parser: Callable[[RuntimeEnv], Tuple[str, UriType]],
        unused_uris_callback: Callable[[List[Tuple[str, UriType]]], None],
        unused_runtime_env_callback: Callable[[str], None],
    ):
        # Runtime Environment reference table. The key is serialized runtime env and
        # the value is reference count.
        self._runtime_env_reference: Dict[str, int] = defaultdict(int)
        # URI reference table. The key is URI parsed from runtime env and the value
        # is reference count.
        self._uri_reference: Dict[str, int] = defaultdict(int)
        self._uris_parser = uris_parser
        self._unused_uris_callback = unused_uris_callback
        self._unused_runtime_env_callback = unused_runtime_env_callback
        # send the `DeleteRuntimeEnvIfPossible` RPC when the client exits. The URI won't
        # be leaked now because the reference count will be reset to zero when the job
        # finished.
        self._reference_exclude_sources: Set[str] = {
            "client_server",
        }

    def _increase_reference_for_uris(self, uris):
        default_logger.debug(f"Increase reference for uris {uris}.")
        for uri, _ in uris:
            self._uri_reference[uri] += 1

    def _decrease_reference_for_uris(self, uris):
        default_logger.debug(f"Decrease reference for uris {uris}.")
        unused_uris = list()
        for uri, uri_type in uris:
            if self._uri_reference[uri] > 0:
                self._uri_reference[uri] -= 1
                if self._uri_reference[uri] == 0:
                    unused_uris.append((uri, uri_type))
                    del self._uri_reference[uri]
            else:
                default_logger.warn(f"URI {uri} does not exist.")
        if unused_uris:
            default_logger.info(f"Unused uris {unused_uris}.")
            self._unused_uris_callback(unused_uris)
        return unused_uris

    def _increase_reference_for_runtime_env(self, serialized_env: str):
        default_logger.debug(f"Increase reference for runtime env {serialized_env}.")
        self._runtime_env_reference[serialized_env] += 1

    def _decrease_reference_for_runtime_env(self, serialized_env: str):
        default_logger.debug(f"Decrease reference for runtime env {serialized_env}.")
        unused = False
        if self._runtime_env_reference[serialized_env] > 0:
            self._runtime_env_reference[serialized_env] -= 1
            if self._runtime_env_reference[serialized_env] == 0:
                unused = True
                del self._runtime_env_reference[serialized_env]
        else:
            default_logger.warn(f"Runtime env {serialized_env} does not exist.")
        if unused:
            default_logger.info(f"Unused runtime env {serialized_env}.")
            self._unused_runtime_env_callback(serialized_env)
        return unused

    def increase_reference(
        self, runtime_env: RuntimeEnv, serialized_env: str, source_process: str
    ) -> None:
        if source_process in self._reference_exclude_sources:
            return
        self._increase_reference_for_runtime_env(serialized_env)
        uris = self._uris_parser(runtime_env)
        self._increase_reference_for_uris(uris)

    def decrease_reference(
        self, runtime_env: RuntimeEnv, serialized_env: str, source_process: str
    ) -> None:
        if source_process in self._reference_exclude_sources:
            return list()
        self._decrease_reference_for_runtime_env(serialized_env)
        uris = self._uris_parser(runtime_env)
        self._decrease_reference_for_uris(uris)

    @property
    def runtime_env_refs(self) -> Dict[str, int]:
        """Return the runtime_env -> ref count mapping.

        Returns:
            The mapping of serialized runtime env -> ref count.
        """
        return self._runtime_env_reference


class RuntimeEnvAgent(
    runtime_env_agent_pb2_grpc.RuntimeEnvServiceServicer,
):
    """An RPC server to create and delete runtime envs.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config.
    """

    def __init__(
        self,
        gcs_address,
        node_ip_address,
        runtime_env_agent_port,
        temp_dir,
        runtime_env_dir,
        logging_params,
        agent_id,
    ):
        self._node_ip_address = node_ip_address
        self._runtime_env_agent_port = runtime_env_agent_port
        self._runtime_env_dir = runtime_env_dir
        self._agent_id = agent_id
        self._logging_params = logging_params
        self._gcs_address = gcs_address

        self._logger = default_logger
        self._logger = setup_component_logger(
            logger_name=default_logger.name, **self._logging_params
        )
        # Don't propagate logs to the root logger, because these logs
        # might contain sensitive information. Instead, these logs should
        # be confined to the runtime env agent log file `self.LOG_FILENAME`.
        self._logger.propagate = False

        # TODO(edoakes): RAY_RAYLET_PID isn't properly set on Windows. This is
        # only used for fate-sharing with the raylet and we need a different
        # fate-sharing mechanism for Windows anyways.
        if sys.platform not in ["win32", "cygwin"]:
            self.ppid = int(os.environ["RAY_RAYLET_PID"])
            assert self.ppid > 0
            self._logger.info("Parent pid is %s", self.ppid)

        # Setup raylet channel
        options = ray_constants.GLOBAL_GRPC_OPTIONS
        self._aiogrpc_raylet_channel = init_grpc_channel(
            f"{self._node_ip_address}:{self._runtime_env_agent_port}",
            options,
            asynchronous=True,
        )
        # Setup grpc server
        self._grpc_server = aiogrpc.server(options=(("grpc.so_reuseport", 0),))
        grpc_ip = "127.0.0.1" if self._node_ip_address == "127.0.0.1" else "0.0.0.0"
        try:
            self._grpc_port = add_port_to_grpc_server(
                self._grpc_server, f"{grpc_ip}:{self._runtime_env_agent_port}"
            )
        except Exception:
            self._logger.exception(
                "Failed to add port to grpc server. Runtime env agent will exit now."
            )
            sys.exit(1)
        else:
            self._logger.info(
                "Runtime env agent grpc address: %s:%s", grpc_ip, self._grpc_port
            )

        # Setup runtime env materials
        self._per_job_logger_cache = dict()
        # Cache the results of creating envs to avoid repeatedly calling into
        # conda and other slow calls.
        self._env_cache: Dict[str, CreatedEnvResult] = dict()
        # Maps a serialized runtime env to a lock that is used
        # to prevent multiple concurrent installs of the same env.
        self._env_locks: Dict[str, asyncio.Lock] = dict()
        self._gcs_aio_client = GcsAioClient(address=self._gcs_address)
        self._pip_plugin = PipPlugin(self._runtime_env_dir)
        self._conda_plugin = CondaPlugin(self._runtime_env_dir)
        self._py_modules_plugin = PyModulesPlugin(
            self._runtime_env_dir, self._gcs_aio_client
        )
        self._java_jars_plugin = JavaJarsPlugin(
            self._runtime_env_dir, self._gcs_aio_client
        )
        self._working_dir_plugin = WorkingDirPlugin(
            self._runtime_env_dir, self._gcs_aio_client
        )
        self._container_manager = ContainerManager(temp_dir)
        # TODO(architkulkarni): "base plugins" and third-party plugins should all go
        # through the same code path.  We should never need to refer to
        # self._xxx_plugin, we should just iterate through self._plugins.
        self._base_plugins: List[RuntimeEnvPlugin] = [
            self._working_dir_plugin,
            self._pip_plugin,
            self._conda_plugin,
            self._py_modules_plugin,
            self._java_jars_plugin,
        ]
        self._plugin_manager = RuntimeEnvPluginManager()
        for plugin in self._base_plugins:
            self._plugin_manager.add_plugin(plugin)

        self._reference_table = ReferenceTable(
            self.uris_parser,
            self.unused_uris_processor,
            self.unused_runtime_env_processor,
        )

    def uris_parser(self, runtime_env):
        result = list()
        for name, plugin_setup_context in self._plugin_manager.plugins.items():
            plugin = plugin_setup_context.class_instance
            uris = plugin.get_uris(runtime_env)
            for uri in uris:
                result.append((uri, UriType(name)))
        return result

    def unused_uris_processor(self, unused_uris: List[Tuple[str, UriType]]) -> None:
        for uri, uri_type in unused_uris:
            self._plugin_manager.plugins[str(uri_type)].uri_cache.mark_unused(uri)

    def unused_runtime_env_processor(self, unused_runtime_env: str) -> None:
        def delete_runtime_env():
            del self._env_cache[unused_runtime_env]
            self._logger.info(
                "Runtime env %s removed from env-level cache.", unused_runtime_env
            )

        if unused_runtime_env in self._env_cache:
            if not self._env_cache[unused_runtime_env].success:
                loop = get_or_create_event_loop()
                # Cache the bad runtime env result by ttl seconds.
                loop.call_later(
                    runtime_env_consts.BAD_RUNTIME_ENV_CACHE_TTL_SECONDS,
                    delete_runtime_env,
                )
            else:
                delete_runtime_env()

    def get_or_create_logger(self, job_id: bytes):
        job_id = job_id.decode()
        if job_id not in self._per_job_logger_cache:
            params = self._logging_params.copy()
            params["filename"] = f"runtime_env_setup-{job_id}.log"
            params["logger_name"] = f"runtime_env_{job_id}"
            params["propagate"] = False
            per_job_logger = setup_component_logger(**params)
            self._per_job_logger_cache[job_id] = per_job_logger
        return self._per_job_logger_cache[job_id]

    async def GetOrCreateRuntimeEnv(self, request, context):
        self._logger.debug(
            f"Got request from {request.source_process} to increase "
            "reference for runtime env: "
            f"{request.serialized_runtime_env}."
        )

        async def _setup_runtime_env(
            runtime_env: RuntimeEnv,
            serialized_runtime_env,
            serialized_allocated_resource_instances,
        ):
            allocated_resource: dict = json.loads(
                serialized_allocated_resource_instances or "{}"
            )
            # Use a separate logger for each job.
            per_job_logger = self.get_or_create_logger(request.job_id)
            # TODO(chenk008): Add log about allocated_resource to
            # avoid lint error. That will be moved to cgroup plugin.
            per_job_logger.debug(f"Worker has resource :" f"{allocated_resource}")
            context = RuntimeEnvContext(env_vars=runtime_env.env_vars())
            await self._container_manager.setup(
                runtime_env, context, logger=per_job_logger
            )

            # Warn about unrecognized fields in the runtime env.
            for name, _ in runtime_env.plugins():
                if name not in self._plugin_manager.plugins:
                    per_job_logger.warning(
                        f"runtime_env field {name} is not recognized by "
                        "Ray and will be ignored.  In the future, unrecognized "
                        "fields in the runtime_env will raise an exception."
                    )

                """Run setup for each plugin unless it has already been cached."""
            for (
                plugin_setup_context
            ) in self._plugin_manager.sorted_plugin_setup_contexts():
                plugin = plugin_setup_context.class_instance
                uri_cache = plugin_setup_context.uri_cache
                await create_for_plugin_if_needed(
                    runtime_env, plugin, uri_cache, context, per_job_logger
                )

            return context

        async def _create_runtime_env_with_retry(
            runtime_env,
            serialized_runtime_env,
            serialized_allocated_resource_instances,
            setup_timeout_seconds,
        ) -> Tuple[bool, str, str]:
            """
            Create runtime env with retry times. This function won't raise exceptions.

            Args:
                runtime_env: The instance of RuntimeEnv class.
                serialized_runtime_env: The serialized runtime env.
                serialized_allocated_resource_instances: The serialized allocated
                resource instances.
                setup_timeout_seconds: The timeout of runtime environment creation.

            Returns:
                a tuple which contains result (bool), runtime env context (str), error
                message(str).

            """
            self._logger.info(
                f"Creating runtime env: {serialized_env} with timeout "
                f"{setup_timeout_seconds} seconds."
            )
            serialized_context = None
            error_message = None
            for _ in range(runtime_env_consts.RUNTIME_ENV_RETRY_TIMES):
                try:
                    runtime_env_setup_task = _setup_runtime_env(
                        runtime_env,
                        serialized_env,
                        request.serialized_allocated_resource_instances,
                    )
                    runtime_env_context = await asyncio.wait_for(
                        runtime_env_setup_task, timeout=setup_timeout_seconds
                    )
                    serialized_context = runtime_env_context.serialize()
                    error_message = None
                    break
                except Exception as e:
                    err_msg = f"Failed to create runtime env {serialized_env}."
                    self._logger.exception(err_msg)
                    error_message = "".join(
                        traceback.format_exception(type(e), e, e.__traceback__)
                    )
                    if isinstance(e, asyncio.TimeoutError):
                        hint = (
                            f"Failed due to timeout; check runtime_env setup logs"
                            " and consider increasing `setup_timeout_seconds` beyond "
                            f"the default of {DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS}."
                            "For example: \n"
                            '    runtime_env={"config": {"setup_timeout_seconds":'
                            " 1800}, ...}\n"
                        )
                        error_message = hint + error_message
                    await asyncio.sleep(
                        runtime_env_consts.RUNTIME_ENV_RETRY_INTERVAL_MS / 1000
                    )
            if error_message:
                self._logger.error(
                    "Runtime env creation failed for %d times, "
                    "don't retry any more.",
                    runtime_env_consts.RUNTIME_ENV_RETRY_TIMES,
                )
                return False, None, error_message
            else:
                self._logger.info(
                    "Successfully created runtime env: %s, the context: %s",
                    serialized_env,
                    serialized_context,
                )
                return True, serialized_context, None

        try:
            serialized_env = request.serialized_runtime_env
            runtime_env = RuntimeEnv.deserialize(serialized_env)
        except Exception as e:
            self._logger.exception(
                "[Increase] Failed to parse runtime env: " f"{serialized_env}"
            )
            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=runtime_env_agent_pb2.RUNTIME_ENV_AGENT_RPC_STATUS_FAILED,
                error_message="".join(
                    traceback.format_exception(type(e), e, e.__traceback__)
                ),
            )

        # Increase reference
        self._reference_table.increase_reference(
            runtime_env, serialized_env, request.source_process
        )

        if serialized_env not in self._env_locks:
            # async lock to prevent the same env being concurrently installed
            self._env_locks[serialized_env] = asyncio.Lock()

        async with self._env_locks[serialized_env]:
            if serialized_env in self._env_cache:
                serialized_context = self._env_cache[serialized_env]
                result = self._env_cache[serialized_env]
                if result.success:
                    context = result.result
                    self._logger.info(
                        "Runtime env already created "
                        f"successfully. Env: {serialized_env}, "
                        f"context: {context}"
                    )
                    return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                        status=RUNTIME_ENV_AGENT_RPC_STATUS_OK,
                        serialized_runtime_env_context=context,
                    )
                else:
                    error_message = result.result
                    self._logger.info(
                        "Runtime env already failed. "
                        f"Env: {serialized_env}, "
                        f"err: {error_message}"
                    )
                    # Recover the reference.
                    self._reference_table.decrease_reference(
                        runtime_env, serialized_env, request.source_process
                    )
                    return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                        status=RUNTIME_ENV_AGENT_RPC_STATUS_FAILED,
                        error_message=error_message,
                    )

            if SLEEP_FOR_TESTING_S:
                self._logger.info(f"Sleeping for {SLEEP_FOR_TESTING_S}s.")
                time.sleep(int(SLEEP_FOR_TESTING_S))

            runtime_env_config = RuntimeEnvConfig.from_proto(request.runtime_env_config)
            # accroding to the document of `asyncio.wait_for`,
            # None means disable timeout logic
            setup_timeout_seconds = (
                None
                if runtime_env_config["setup_timeout_seconds"] == -1
                else runtime_env_config["setup_timeout_seconds"]
            )

            start = time.perf_counter()
            (
                successful,
                serialized_context,
                error_message,
            ) = await _create_runtime_env_with_retry(
                runtime_env,
                serialized_env,
                request.serialized_allocated_resource_instances,
                setup_timeout_seconds,
            )
            creation_time_ms = int(round((time.perf_counter() - start) * 1000, 0))
            if not successful:
                # Recover the reference.
                self._reference_table.decrease_reference(
                    runtime_env, serialized_env, request.source_process
                )
            # Add the result to env cache.
            self._env_cache[serialized_env] = CreatedEnvResult(
                successful,
                serialized_context if successful else error_message,
                creation_time_ms,
            )
            # Reply the RPC
            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=RUNTIME_ENV_AGENT_RPC_STATUS_OK
                if successful
                else RUNTIME_ENV_AGENT_RPC_STATUS_FAILED,
                serialized_runtime_env_context=serialized_context,
                error_message=error_message,
            )

    async def DeleteRuntimeEnvIfPossible(self, request, context):
        self._logger.info(
            f"Got request from {request.source_process} to decrease "
            "reference for runtime env: "
            f"{request.serialized_runtime_env}."
        )

        try:
            runtime_env = RuntimeEnv.deserialize(request.serialized_runtime_env)
        except Exception as e:
            self._logger.exception(
                "[Decrease] Failed to parse runtime env: "
                f"{request.serialized_runtime_env}"
            )
            return runtime_env_agent_pb2.GetOrCreateRuntimeEnvReply(
                status=RUNTIME_ENV_AGENT_RPC_STATUS_FAILED,
                error_message="".join(
                    traceback.format_exception(type(e), e, e.__traceback__)
                ),
            )

        self._reference_table.decrease_reference(
            runtime_env, request.serialized_runtime_env, request.source_process
        )

        return runtime_env_agent_pb2.DeleteRuntimeEnvIfPossibleReply(
            status=RUNTIME_ENV_AGENT_RPC_STATUS_OK
        )

    async def GetRuntimeEnvsInfo(self, request, context):
        """Return the runtime env information of the node."""
        # TODO(sang): Currently, it only includes runtime_env information.
        # We should include the URI information which includes,
        # URIs
        # Caller
        # Ref counts
        # Cache information
        # Metrics (creation time & success)
        # Deleted URIs
        limit = request.limit if request.HasField("limit") else -1
        runtime_env_states = defaultdict(ProtoRuntimeEnvState)
        runtime_env_refs = self._reference_table.runtime_env_refs
        for runtime_env, ref_cnt in runtime_env_refs.items():
            runtime_env_states[runtime_env].runtime_env = runtime_env
            runtime_env_states[runtime_env].ref_cnt = ref_cnt
        for runtime_env, result in self._env_cache.items():
            runtime_env_states[runtime_env].runtime_env = runtime_env
            runtime_env_states[runtime_env].success = result.success
            if not result.success:
                runtime_env_states[runtime_env].error = result.result
            runtime_env_states[runtime_env].creation_time_ms = result.creation_time_ms

        reply = runtime_env_agent_pb2.GetRuntimeEnvsInfoReply()
        count = 0
        for runtime_env_state in runtime_env_states.values():
            if limit != -1 and count >= limit:
                break
            count += 1
            reply.runtime_env_states.append(runtime_env_state)
        reply.total = len(runtime_env_states)
        return reply

    async def run(self):
        async def _check_parent():
            """Check if raylet is dead and fate-share if it is."""
            try:
                curr_proc = psutil.Process()
                parent_death_cnt = 0
                while True:
                    parent = curr_proc.parent()
                    # If the parent is dead, it is None.
                    parent_gone = parent is None
                    init_assigned_for_parent = False
                    parent_changed = False

                    if parent:
                        # Sometimes, the parent is changed to the `init` process.
                        # In this case, the parent.pid is 1.
                        init_assigned_for_parent = parent.pid == 1
                        # Sometimes, the parent is dead, and the pid is reused
                        # by other processes. In this case, this condition is triggered.
                        parent_changed = self.ppid != parent.pid

                    if parent_gone or init_assigned_for_parent or parent_changed:
                        parent_death_cnt += 1
                        logger.warning(
                            f"Raylet is considered dead {parent_death_cnt} X. "
                            f"If it reaches to {_PARENT_DEATH_THREASHOLD}, the agent "
                            f"will kill itself. Parent: {parent}, "
                            f"parent_gone: {parent_gone}, "
                            f"init_assigned_for_parent: {init_assigned_for_parent}, "
                            f"parent_changed: {parent_changed}."
                        )
                        if parent_death_cnt < _PARENT_DEATH_THREASHOLD:
                            await asyncio.sleep(
                                runtime_env_consts.CHECK_PARENT_INTERVAL_S
                            )
                            continue
                        sys.exit(0)
                    else:
                        parent_death_cnt = 0
                    await asyncio.sleep(runtime_env_consts.CHECK_PARENT_INTERVAL_S)
            except Exception:
                logger.exception("Failed to check parent PID, exiting.")
                sys.exit(1)

        if sys.platform not in ["win32", "cygwin"]:
            check_parent_task = create_task(_check_parent())

        # Start a grpc asyncio server.
        assert self._grpc_server is not None
        await self._grpc_server.start()
        runtime_env_agent_pb2_grpc.add_RuntimeEnvServiceServicer_to_server(
            self, self._grpc_server
        )
        # Register agent to agent manager.
        raylet_stub = (
            runtime_env_agent_manager_pb2_grpc.RuntimeEnvAgentManagerServiceStub(
                self._aiogrpc_raylet_channel
            )
        )
        await raylet_stub.RegisterRuntimeEnvAgent(
            runtime_env_agent_manager_pb2.RegisterRuntimeEnvAgentRequest(
                agent_id=self._agent_id,
                agent_port=self._grpc_port,
                agent_ip_address=self._node_ip_address,
            )
        )
        tasks = [check_parent_task]
        await asyncio.gather(*tasks)
        await self.server.wait_for_termination()

    @staticmethod
    def is_minimal_module():
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runtime env agent.")
    parser.add_argument(
        "--gcs-address", required=True, type=str, help="The address (ip:port) of GCS."
    )
    parser.add_argument(
        "--node-ip-address",
        required=True,
        type=str,
        help="the IP address of this node.",
    )
    parser.add_argument(
        "--runtime-env-agent-port",
        required=True,
        type=int,
        help="The port on which the runtime env agent will receive GRPCs.",
    )
    parser.add_argument(
        "--logging-level",
        required=False,
        type=lambda s: logging.getLevelName(s.upper()),
        default=ray_constants.LOGGER_LEVEL,
        choices=ray_constants.LOGGER_LEVEL_CHOICES,
        help=ray_constants.LOGGER_LEVEL_HELP,
    )
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=ray_constants.LOGGER_FORMAT,
        help=ray_constants.LOGGER_FORMAT_HELP,
    )
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=runtime_env_consts.RUNTIME_ENV_AGENT_DEFAULT_LOG_FILENAME,
        help="Specify the name of log file, "
        'log to stdout if set empty, default is "{}".'.format(
            runtime_env_consts.RUNTIME_ENV_AGENT_DEFAULT_LOG_FILENAME
        ),
    )
    parser.add_argument(
        "--logging-rotate-bytes",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is {} bytes.".format(ray_constants.LOGGING_ROTATE_BYTES),
    )
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is {}.".format(
            ray_constants.LOGGING_ROTATE_BACKUP_COUNT
        ),
    )
    parser.add_argument(
        "--log-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of log directory.",
    )
    parser.add_argument(
        "--temp-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the temporary directory use by Ray process.",
    )

    parser.add_argument(
        "--runtime-env-dir",
        required=True,
        type=str,
        default=None,
        help="Specify the path of the resource directory used by runtime_env.",
    )
    parser.add_argument(
        "--agent-id",
        required=True,
        type=int,
        help="ID to report when registering with raylet",
        default=os.getpid(),
    )
    args = parser.parse_args()

    try:
        logging_params = dict(
            logging_level=args.logging_level,
            logging_format=args.logging_format,
            log_dir=args.log_dir,
            filename=args.logging_filename,
            max_bytes=args.logging_rotate_bytes,
            backup_count=args.logging_rotate_backup_count,
        )

        logger = setup_component_logger(**logging_params)

        # Initialize event loop, see Dashboard init code for caveat
        # w.r.t grpc server init in the DashboardAgent initializer.
        agent = RuntimeEnvAgent(
            gcs_address=args.gcs_address,
            node_ip_address=args.node_ip_address,
            runtime_env_agent_port=args.runtime_env_agent_port,
            temp_dir=args.temp_dir,
            runtime_env_dir=args.runtime_env_dir,
            logging_params=logging_params,
            agent_id=args.agent_id,
        )

        loop = get_or_create_event_loop()

        def sigterm_handler():
            logger.warning("Exiting with SIGTERM immediately...")
            # Exit code 0 will be considered as an expected shutdown
            os._exit(signal.SIGTERM)

        if sys.platform != "win32":
            # TODO(rickyyx): we currently do not have any logic for actual
            # graceful termination in the agent. Most of the underlying
            # async tasks run by the agent head doesn't handle CancelledError.
            # So a truly graceful shutdown is not trivial w/o much refactoring.
            # Re-open the issue: https://github.com/ray-project/ray/issues/25518
            # if a truly graceful shutdown is required.
            loop.add_signal_handler(signal.SIGTERM, sigterm_handler)

        loop.run_until_complete(agent.run())
    except Exception:
        logger.exception("Agent is working abnormally. It will exit immediately.")
        exit(1)
