import os
import sys
import time
import logging
import psutil

import pytest

import ray

from typing import List, Tuple
from ray._private.runtime_env.runtime_env_agent.runtime_env_agent import (
    UriType,
    ReferenceTable,
)
from ray.tests.conftest import *  # noqa
from ray._private import ray_constants
from ray._private.test_utils import (
    get_error_message,
    init_error_pubsub,
    wait_for_condition,
)
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)


conflict_port = 34567


def run_tasks_without_runtime_env():
    assert ray.is_initialized()

    @ray.remote
    def f():
        pass

    for _ in range(10):
        time.sleep(1)
        ray.get(f.remote())


def run_tasks_with_runtime_env():
    assert ray.is_initialized()

    @ray.remote(runtime_env={"pip": ["pip-install-test==0.5"]})
    def f():
        import pip_install_test  # noqa

        pass

    for _ in range(3):
        time.sleep(1)
        ray.get(f.remote())


@pytest.mark.skipif(
    sys.platform == "win32", reason="`runtime_env` with `pip` not supported on Windows."
)
@pytest.mark.parametrize(
    "listen_port",
    [conflict_port],
    indirect=True,
)
@pytest.mark.parametrize(
    "call_ray_start",
    [f"ray start --head --num-cpus=1 --runtime-env-agent-port={conflict_port}"],
    indirect=True,
)
def test_runtime_env_agent_grpc_port_conflict(listen_port, call_ray_start):
    address = call_ray_start
    ray.init(address=address)

    # Tasks without runtime env still work when runtime env agent grpc port conflicts.
    run_tasks_without_runtime_env()
    # Tasks with runtime env couldn't work.
    with pytest.raises(
        ray.exceptions.RuntimeEnvSetupError,
        match="Ray agent couldn't be started due to the port conflict",
    ):
        run_tasks_with_runtime_env()


def test_reference_table():
    expected_unused_uris = []
    expected_unused_runtime_env = str()

    def uris_parser(runtime_env) -> Tuple[str, UriType]:
        result = list()
        result.append((runtime_env.working_dir(), "working_dir"))
        py_module_uris = runtime_env.py_modules()
        for uri in py_module_uris:
            result.append((uri, "py_modules"))
        return result

    def unused_uris_processor(unused_uris: List[Tuple[str, UriType]]) -> None:
        nonlocal expected_unused_uris
        assert expected_unused_uris
        for unused in unused_uris:
            assert unused in expected_unused_uris
            expected_unused_uris.remove(unused)
        assert not expected_unused_uris

    def unused_runtime_env_processor(unused_runtime_env: str) -> None:
        nonlocal expected_unused_runtime_env
        assert expected_unused_runtime_env
        assert expected_unused_runtime_env == unused_runtime_env
        expected_unused_runtime_env = None

    reference_table = ReferenceTable(
        uris_parser, unused_uris_processor, unused_runtime_env_processor
    )
    runtime_env_1 = RuntimeEnv(
        working_dir="s3://working_dir_1.zip",
        py_modules=["s3://py_module_A.zip", "s3://py_module_B.zip"],
    )
    runtime_env_2 = RuntimeEnv(
        working_dir="s3://working_dir_2.zip",
        py_modules=["s3://py_module_A.zip", "s3://py_module_C.zip"],
    )
    # Add runtime env 1
    reference_table.increase_reference(
        runtime_env_1, runtime_env_1.serialize(), "raylet"
    )
    # Add runtime env 2
    reference_table.increase_reference(
        runtime_env_2, runtime_env_2.serialize(), "raylet"
    )
    # Add runtime env 1 by `client_server`, this will be skipped by reference table.
    reference_table.increase_reference(
        runtime_env_1, runtime_env_1.serialize(), "client_server"
    )

    # Remove runtime env 1
    expected_unused_uris.append(("s3://working_dir_1.zip", "working_dir"))
    expected_unused_uris.append(("s3://py_module_B.zip", "py_modules"))
    expected_unused_runtime_env = runtime_env_1.serialize()
    reference_table.decrease_reference(
        runtime_env_1, runtime_env_1.serialize(), "raylet"
    )
    assert not expected_unused_uris
    assert not expected_unused_runtime_env

    # Remove runtime env 2
    expected_unused_uris.append(("s3://working_dir_2.zip", "working_dir"))
    expected_unused_uris.append(("s3://py_module_A.zip", "py_modules"))
    expected_unused_uris.append(("s3://py_module_C.zip", "py_modules"))
    expected_unused_runtime_env = runtime_env_2.serialize()
    reference_table.decrease_reference(
        runtime_env_2, runtime_env_2.serialize(), "raylet"
    )
    assert not expected_unused_uris
    assert not expected_unused_runtime_env


def search_runtime_env_agent(processes):
    for p in processes:
        try:
            for c in p.cmdline():
                if os.path.join("runtime_env_agent", "runtime_env_agent.py") in c:
                    return p
        except Exception:
            pass


def check_agent_register(raylet_proc, agent_pid):
    # Check if agent register is OK.
    for x in range(5):
        logger.info("Check agent is alive.")
        agent_proc = search_runtime_env_agent(raylet_proc.children())
        assert agent_proc.pid == agent_pid
        time.sleep(1)


@pytest.mark.skipif(
    sys.platform in ["win32", "cygwin"], reason="fate-share is not supported on windows"
)
def test_raylet_and_agent_share_fate(shutdown_only):
    """Test raylet and runtime env agent share fate."""

    ray.init(include_dashboard=False)
    p = init_error_pubsub()

    node = ray._private.worker._global_node
    all_processes = node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)

    wait_for_condition(lambda: search_runtime_env_agent(raylet_proc.children()))
    agent_proc = search_runtime_env_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The agent should be dead if raylet exits.
    raylet_proc.terminate()
    raylet_proc.wait()
    agent_proc.wait(15)

    # No error should be reported for graceful termination.
    errors = get_error_message(p, 1, ray_constants.RAYLET_DIED_ERROR)
    assert len(errors) == 0, errors

    ray.shutdown()

    ray.init(include_dashboard=False)
    all_processes = ray._private.worker._global_node.all_processes
    raylet_proc_info = all_processes[ray_constants.PROCESS_TYPE_RAYLET][0]
    raylet_proc = psutil.Process(raylet_proc_info.process.pid)
    wait_for_condition(lambda: search_runtime_env_agent(raylet_proc.children()))
    agent_proc = search_runtime_env_agent(raylet_proc.children())
    agent_pid = agent_proc.pid

    check_agent_register(raylet_proc, agent_pid)

    # The raylet should be dead if agent exits.
    agent_proc.kill()
    agent_proc.wait()
    raylet_proc.wait(15)


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
