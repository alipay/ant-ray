import asyncio
import itertools
import json
import logging
import subprocess
import textwrap
import types
from typing import Dict, List, Tuple, Any

from google.protobuf import json_format
from ray.core.generated.runtime_env_common_pb2 import RuntimeEnv as ProtoRuntimeEnv


def _build_proto_pip_runtime_env(runtime_env_dict: dict, runtime_env: ProtoRuntimeEnv):
    """Construct pip runtime env protobuf from runtime env dict."""
    if runtime_env_dict.get("pip"):
        runtime_env.python_runtime_env.pip_runtime_env.config.packages.extend(
            runtime_env_dict["pip"]
        )


def _parse_proto_pip_runtime_env(runtime_env: ProtoRuntimeEnv, runtime_env_dict: dict):
    """Parse pip runtime env protobuf to runtime env dict."""
    if runtime_env.python_runtime_env.HasField("pip_runtime_env"):
        runtime_env_dict["pip"] = list(
            runtime_env.python_runtime_env.pip_runtime_env.config.packages
        )


def _build_proto_conda_runtime_env(
    runtime_env_dict: dict, runtime_env: ProtoRuntimeEnv
):
    """Construct conda runtime env protobuf from runtime env dict."""
    if runtime_env_dict.get("conda"):
        if isinstance(runtime_env_dict["conda"], str):
            runtime_env.python_runtime_env.conda_runtime_env.conda_env_name = (
                runtime_env_dict["conda"]
            )
        else:
            runtime_env.python_runtime_env.conda_runtime_env.config = json.dumps(
                runtime_env_dict["conda"], sort_keys=True
            )


def _parse_proto_conda_runtime_env(
    runtime_env: ProtoRuntimeEnv, runtime_env_dict: dict
):
    """Parse conda runtime env protobuf to runtime env dict."""
    if runtime_env.python_runtime_env.HasField("conda_runtime_env"):
        runtime_env_dict["conda"] = json.loads(
            runtime_env.python_runtime_env.conda_runtime_env.config
        )


def _build_proto_container_runtime_env(
    runtime_env_dict: dict, runtime_env: ProtoRuntimeEnv
):
    """Construct container runtime env protobuf from runtime env dict."""
    if runtime_env_dict.get("container"):
        container = runtime_env_dict["container"]
        runtime_env.python_runtime_env.container_runtime_env.image = container.get(
            "image", ""
        )
        runtime_env.python_runtime_env.container_runtime_env.worker_path = (
            container.get("worker_path", "")
        )
        runtime_env.python_runtime_env.container_runtime_env.run_options.extend(
            container.get("run_options", [])
        )


def _parse_proto_container_runtime_env(
    runtime_env: ProtoRuntimeEnv, runtime_env_dict: dict
):
    """Parse container runtime env protobuf to runtime env dict."""
    if runtime_env.python_runtime_env.HasField("container_runtime_env"):
        runtime_env_dict["container"][
            "image"
        ] = runtime_env.python_runtime_env.container_runtime_env.image
        runtime_env_dict["container"][
            "worker_path"
        ] = runtime_env.python_runtime_env.container_runtime_env.worker_path
        runtime_env_dict["container"]["run_options"] = list(
            runtime_env.python_runtime_env.container_runtime_env.run_options
        )


def _build_proto_plugin_runtime_env(
    runtime_env_dict: dict, runtime_env: ProtoRuntimeEnv
):
    """Construct plugin runtime env protobuf from runtime env dict."""
    if runtime_env_dict.get("plugins"):
        for class_path, plugin_field in runtime_env_dict["plugins"].items():
            plugin = runtime_env.python_runtime_env.plugin_runtime_env.plugins.add()
            plugin.class_path = class_path
            plugin.config = json.dumps(plugin_field, sort_keys=True)


def _parse_proto_plugin_runtime_env(
    runtime_env: ProtoRuntimeEnv, runtime_env_dict: dict
):
    """Parse plugin runtime env protobuf to runtime env dict."""
    if runtime_env.python_runtime_env.HasField("plugin_runtime_env"):
        for plugin in runtime_env.python_runtime_env.plugin_runtime_env.plugins:
            runtime_env_dict["plugins"][plugin.class_path] = dict(
                json.loads(plugin.config)
            )


class RuntimeEnv:
    """
    A wrap class of runtime env protobuf.
    """

    def __init__(
        self, serialized_runtime_env=None, proto_runtime_env: ProtoRuntimeEnv = None
    ):
        if serialized_runtime_env:
            self._proto_runtime_env = json_format.Parse(
                serialized_runtime_env, ProtoRuntimeEnv()
            )
        elif proto_runtime_env:
            self._proto_runtime_env = proto_runtime_env
        else:
            self._proto_runtime_env = ProtoRuntimeEnv()

    def to_dict(self) -> Dict:
        initialize_dict: Dict[str, Any] = {}
        if self._proto_runtime_env.python_runtime_env.py_modules:
            initialize_dict["py_modules"] = list(
                self._proto_runtime_env.python_runtime_env.py_modules
            )
        if self._proto_runtime_env.working_dir:
            initialize_dict["working_dir"] = self._proto_runtime_env.working_dir
        if self._proto_runtime_env.env_vars:
            initialize_dict["env_vars"] = dict(self._proto_runtime_env.env_vars)
        if self._proto_runtime_env.extensions:
            initialize_dict.update(dict(self._proto_runtime_env.extensions))
        _parse_proto_pip_runtime_env(self._proto_runtime_env, initialize_dict)
        _parse_proto_conda_runtime_env(self._proto_runtime_env, initialize_dict)
        _parse_proto_container_runtime_env(self._proto_runtime_env, initialize_dict)
        _parse_proto_plugin_runtime_env(self._proto_runtime_env, initialize_dict)
        return initialize_dict

    def has_uris(self) -> bool:
        uris = self._proto_runtime_env.uris
        if (
            uris.working_dir_uri
            or uris.py_modules_uris
            or uris.conda_uri
            or uris.pip_uri
            or uris.plugin_uris
        ):
            return True
        return False

    def working_dir_uri(self) -> str:
        return self._proto_runtime_env.uris.working_dir_uri

    def py_modules_uris(self) -> List[str]:
        return list(self._proto_runtime_env.uris.py_modules_uris)

    def conda_uri(self) -> str:
        return self._proto_runtime_env.uris.conda_uri

    def pip_uri(self) -> str:
        return self._proto_runtime_env.uris.pip_uri

    def plugin_uris(self) -> List[str]:
        return list(self._proto_runtime_env.uris.plugin_uris)

    def working_dir(self) -> str:
        return self._proto_runtime_env.working_dir

    def py_modules(self) -> List[str]:
        return list(self._proto_runtime_env.python_runtime_env.py_modules)

    def env_vars(self) -> Dict:
        return dict(self._proto_runtime_env.env_vars)

    def plugins(self) -> List[Tuple[str, str]]:
        result = list()
        for (
            plugin
        ) in self._proto_runtime_env.python_runtime_env.plugin_runtime_env.plugins:
            result.append((plugin.class_path, plugin.config))
        return result

    def has_conda(self) -> str:
        return self._proto_runtime_env.python_runtime_env.HasField("conda_runtime_env")

    def conda_env_name(self) -> str:
        if not self.has_conda():
            return None
        if not self._proto_runtime_env.python_runtime_env.conda_runtime_env.HasField(
            "conda_env_name"
        ):
            return None
        return (
            self._proto_runtime_env.python_runtime_env.conda_runtime_env.conda_env_name
        )

    def conda_config(self) -> str:
        if not self.has_conda():
            return None
        if not self._proto_runtime_env.python_runtime_env.conda_runtime_env.HasField(
            "config"
        ):
            return None
        return self._proto_runtime_env.python_runtime_env.conda_runtime_env.config

    def has_pip(self) -> bool:
        return self._proto_runtime_env.python_runtime_env.HasField("pip_runtime_env")

    def pip_packages(self) -> List:
        if not self.has_pip():
            return []
        return list(
            self._proto_runtime_env.python_runtime_env.pip_runtime_env.config.packages
        )

    def serialize(self) -> str:
        # Sort the keys we can compare the serialized string for equality.
        return json.dumps(
            json.loads(json_format.MessageToJson(self._proto_runtime_env)),
            sort_keys=True,
        )

    def get_extension(self, key) -> str:
        return self._proto_runtime_env.extensions.get(key)

    def has_py_container(self) -> bool:
        return self._proto_runtime_env.python_runtime_env.HasField(
            "container_runtime_env"
        )

    def py_container_image(self) -> str:
        if not self.has_py_container():
            return None
        return self._proto_runtime_env.python_runtime_env.container_runtime_env.image

    def py_container_run_options(self) -> List:
        if not self.has_py_container():
            return None
        return list(
            self._proto_runtime_env.python_runtime_env.container_runtime_env.run_options
        )

    @classmethod
    def from_dict(
        cls, runtime_env_dict: Dict[str, Any], conda_get_uri_fn, pip_get_uri_fn
    ) -> "RuntimeEnv":
        proto_runtime_env = ProtoRuntimeEnv()
        proto_runtime_env.working_dir = runtime_env_dict.get("working_dir", "")
        if "working_dir" in runtime_env_dict:
            proto_runtime_env.uris.working_dir_uri = runtime_env_dict["working_dir"]
        if "py_modules" in runtime_env_dict:
            proto_runtime_env.python_runtime_env.py_modules.extend(
                runtime_env_dict["py_modules"]
            )
            for uri in runtime_env_dict["py_modules"]:
                proto_runtime_env.uris.py_modules_uris.append(uri)
        if "conda" in runtime_env_dict:
            uri = conda_get_uri_fn(runtime_env_dict)
            if uri is not None:
                proto_runtime_env.uris.conda_uri = uri
        if "pip" in runtime_env_dict:
            uri = pip_get_uri_fn(runtime_env_dict)
            if uri is not None:
                proto_runtime_env.uris.pip_uri = uri
        env_vars = runtime_env_dict.get("env_vars", {})
        proto_runtime_env.env_vars.update(env_vars.items())
        if "_ray_release" in runtime_env_dict:
            proto_runtime_env.extensions["_ray_release"] = str(
                runtime_env_dict["_ray_release"]
            )
        if "_ray_commit" in runtime_env_dict:
            proto_runtime_env.extensions["_ray_commit"] = str(
                runtime_env_dict["_ray_commit"]
            )
        if "_inject_current_ray" in runtime_env_dict:
            proto_runtime_env.extensions["_inject_current_ray"] = str(
                runtime_env_dict["_inject_current_ray"]
            )
        _build_proto_pip_runtime_env(runtime_env_dict, proto_runtime_env)
        _build_proto_conda_runtime_env(runtime_env_dict, proto_runtime_env)
        _build_proto_container_runtime_env(runtime_env_dict, proto_runtime_env)
        _build_proto_plugin_runtime_env(runtime_env_dict, proto_runtime_env)
        return cls(proto_runtime_env=proto_runtime_env)


class SubprocessCalledProcessError(subprocess.CalledProcessError):
    """The subprocess.CalledProcessError with stripped stdout."""

    LAST_N_LINES = 50

    def __init__(self, *args, cmd_index=None, **kwargs):
        self.cmd_index = cmd_index
        super().__init__(*args, **kwargs)

    @staticmethod
    def _get_last_n_line(str_data: str, last_n_lines: int) -> str:
        if last_n_lines < 0:
            return str_data
        lines = str_data.strip().split("\n")
        return "\n".join(lines[-last_n_lines:])

    def __str__(self):
        str_list = (
            []
            if self.cmd_index is None
            else [f"Run cmd[{self.cmd_index}] failed with the following details."]
        )
        str_list.append(super().__str__())
        out = {
            "stdout": self.stdout,
            "stderr": self.stderr,
        }
        for name, s in out.items():
            if s:
                subtitle = f"Last {self.LAST_N_LINES} lines of {name}:"
                last_n_line_str = self._get_last_n_line(s, self.LAST_N_LINES).strip()
                str_list.append(
                    f"{subtitle}\n{textwrap.indent(last_n_line_str, ' ' * 4)}"
                )
        return "\n".join(str_list)


async def check_output_cmd(
    cmd: List[str],
    *,
    logger: logging.Logger,
    cmd_index_gen: types.GeneratorType = itertools.count(1),
    **kwargs,
) -> str:
    """Run command with arguments and return its output.

    If the return code was non-zero it raises a CalledProcessError. The
    CalledProcessError object will have the return code in the returncode
    attribute and any output in the output attribute.

    Args:
        cmd: The cmdline should be a sequence of program arguments or else
            a single string or path-like object. The program to execute is
            the first item in cmd.
        logger: The logger instance.
        cmd_index_gen: The cmd index generator, default is itertools.count(1).
        kwargs: All arguments are passed to the create_subprocess_exec.

    Returns:
        The stdout of cmd.

    Raises:
        CalledProcessError: If the return code of cmd is not 0.
    """

    cmd_index = next(cmd_index_gen)
    logger.info("Run cmd[%s] %s", cmd_index, repr(cmd))

    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            **kwargs,
        )
        # Use communicate instead of polling stdout:
        #   * Avoid deadlocks due to streams pausing reading or writing and blocking the
        #     child process. Please refer to:
        #     https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.asyncio.subprocess.Process.stderr
        #   * Avoid mixing multiple outputs of concurrent cmds.
        stdout, _ = await proc.communicate()
    except BaseException as e:
        raise RuntimeError(f"Run cmd[{cmd_index}] got exception.") from e
    else:
        stdout = stdout.decode("utf-8")
        if stdout:
            logger.info("Output of cmd[%s]: %s", cmd_index, stdout)
        else:
            logger.info("No output for cmd[%s]", cmd_index)
        if proc.returncode != 0:
            raise SubprocessCalledProcessError(
                proc.returncode, cmd, output=stdout, cmd_index=cmd_index
            )
        return stdout
    finally:
        if proc is not None:
            # Kill process.
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            # Wait process exit.
            await proc.wait()
