import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
import ray._private.runtime_env.constants as runtime_env_constants
from ray._private.runtime_env.packaging import (
    Protocol,
    delete_package,
    download_and_unpack_package,
    get_local_dir_from_uri,
    get_uri_for_directory,
    get_uri_for_package,
    parse_uri,
    upload_package_if_needed,
    upload_package_to_gcs,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import get_directory_size_bytes, try_to_create_directory
from ray.exceptions import RuntimeEnvSetupError

default_logger = logging.getLogger(__name__)

_WIN32 = os.name == "nt"


def upload_working_dir_if_needed(
    runtime_env: Dict[str, Any],
    scratch_dir: Optional[str] = os.getcwd(),
    logger: Optional[logging.Logger] = default_logger,
    upload_fn=None,
) -> Dict[str, Any]:
    """Uploads the working_dir and replaces it with a URI.

    If the working_dir is already a URI, this is a no-op.
    """
    working_dir = runtime_env.get("working_dir")
    if working_dir is None:
        return runtime_env

    if not isinstance(working_dir, str) and not isinstance(working_dir, Path):
        raise TypeError(
            "working_dir must be a string or Path (either a local path "
            f"or remote URI), got {type(working_dir)}."
        )

    if isinstance(working_dir, Path):
        working_dir = str(working_dir)

    # working_dir is already a URI -- just pass it through.
    try:
        protocol, path = parse_uri(working_dir)
    except ValueError:
        protocol, path = None, None

    if protocol is not None:
        if protocol in Protocol.remote_protocols() and not path.endswith(".zip"):
            raise ValueError("Only .zip files supported for remote URIs.")
        return runtime_env

    excludes = runtime_env.get("excludes", None)
    try:
        working_dir_uri = get_uri_for_directory(working_dir, excludes=excludes)
    except ValueError:  # working_dir is not a directory
        package_path = Path(working_dir)
        if not package_path.exists() or package_path.suffix != ".zip":
            raise ValueError(
                f"directory {package_path} must be an existing "
                "directory or a zip package"
            )

        pkg_uri = get_uri_for_package(package_path)
        try:
            upload_package_to_gcs(pkg_uri, package_path.read_bytes())
        except Exception as e:
            raise RuntimeEnvSetupError(
                f"Failed to upload package {package_path} to the Ray cluster: {e}"
            ) from e
        runtime_env["working_dir"] = pkg_uri
        return runtime_env
    if upload_fn is None:
        try:
            upload_package_if_needed(
                working_dir_uri,
                scratch_dir,
                working_dir,
                include_parent_dir=False,
                excludes=excludes,
                logger=logger,
            )
        except Exception as e:
            raise RuntimeEnvSetupError(
                f"Failed to upload working_dir {working_dir} to the Ray cluster: {e}"
            ) from e
    else:
        upload_fn(working_dir, excludes=excludes)

    runtime_env["working_dir"] = working_dir_uri
    return runtime_env


def set_pythonpath_in_context(python_path: str, context: RuntimeEnvContext):
    """Insert the path as the first entry in PYTHONPATH in the runtime env.

    This is compatible with users providing their own PYTHONPATH in env_vars,
    and is also compatible with the existing PYTHONPATH in the cluster.

    The import priority is as follows:
    this python_path arg > env_vars PYTHONPATH > existing cluster env PYTHONPATH.
    """
    if "PYTHONPATH" in context.env_vars:
        python_path += os.pathsep + context.env_vars["PYTHONPATH"]
    if "PYTHONPATH" in os.environ:
        python_path += os.pathsep + os.environ["PYTHONPATH"]
    context.env_vars["PYTHONPATH"] = python_path


class WorkingDirPlugin(RuntimeEnvPlugin):

    name = "working_dir"
    working_dir_placeholder = "$WORKING_DIR_PLACEHOLDER"

    def __init__(
        self, resources_dir: str, gcs_aio_client: "GcsAioClient"  # noqa: F821
    ):
        self._resources_dir = os.path.join(resources_dir, "working_dir_files")
        self._working_dirs = os.path.join(resources_dir, "working_dirs")
        self._gcs_aio_client = gcs_aio_client
        try_to_create_directory(self._resources_dir)

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        """Delete URI and return the number of bytes deleted."""
        logger.info("Got request to delete working dir URI %s", uri)
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        local_dir_size = get_directory_size_bytes(local_dir)

        deleted = delete_package(uri, self._resources_dir)
        if not deleted:
            logger.warning(f"Tried to delete nonexistent URI: {uri}.")
            return 0

        return local_dir_size

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        working_dir_uri = runtime_env.working_dir()
        if working_dir_uri != "":
            return [working_dir_uri]
        return []

    async def create(
        self,
        uri: Optional[str],
        runtime_env: dict,
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ) -> int:
        local_dir = await download_and_unpack_package(
            uri, self._resources_dir, self._gcs_aio_client, logger=logger
        )
        return get_directory_size_bytes(local_dir)

    def modify_context(
        self,
        uris: List[str],
        runtime_env_dict: Dict,
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not uris:
            return

        # WorkingDirPlugin uses a single URI.
        uri = uris[0]
        local_dir = get_local_dir_from_uri(uri, self._resources_dir)
        if not local_dir.exists():
            raise ValueError(
                f"Local directory {local_dir} for URI {uri} does "
                "not exist on the cluster. Something may have gone wrong while "
                "downloading or unpacking the working_dir."
            )

        # Use placeholder here and will replace it by `pre_worker_startup`.
        if not _WIN32:
            context.command_prefix += [
                "cd",
                WorkingDirPlugin.working_dir_placeholder,
                "&&",
            ]
        else:
            # Include '/d' incase temp folder is on different drive than Ray install.
            context.command_prefix += [
                "cd",
                "/d",
                WorkingDirPlugin.working_dir_placeholder,
                "&&",
            ]
        set_pythonpath_in_context(
            python_path=WorkingDirPlugin.working_dir_placeholder, context=context
        )
        context.cwd = WorkingDirPlugin.working_dir_placeholder
        context.symlink_dirs_to_cwd.append(str(local_dir))

    async def pre_worker_startup(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        worker_id: str,
    ) -> None:
        if not runtime_env.working_dir():
            return
        default_logger.info(f"Creating working dir for worker {worker_id}.")
        # TODO(SongGuyang): Currently, we create working dir for each worker process.
        # But when multiple tasks share one process, the working dir still can't be
        # isolated between different tasks. We should achieve absolute working dir
        # isolation for both tasks and actors, instead of process level isolation.
        working_dir = os.path.join(self._working_dirs, worker_id)
        os.makedirs(working_dir, exist_ok=True)
        context.cwd = working_dir
        # Replace the placeholder with the real working dir.
        for i, prefix in enumerate(context.command_prefix):
            context.command_prefix[i] = prefix.replace(
                WorkingDirPlugin.working_dir_placeholder, working_dir
            )
        for k, v in context.env_vars.copy().items():
            context.env_vars[k] = v.replace(
                WorkingDirPlugin.working_dir_placeholder, working_dir
            )
        if "container_command" in context.container:
            for i, command_str in enumerate(context.container["container_command"]):
                context.container["container_command"][i] = command_str.replace(
                    WorkingDirPlugin.working_dir_placeholder, working_dir
                )
        # Add symbol links to the working dir.
        for symlink_dir in context.symlink_dirs_to_cwd:
            for name in os.listdir(symlink_dir):
                src_path = os.path.join(symlink_dir, name)
                link_path = os.path.join(working_dir, name)
                default_logger.info(f"Creating symlink from {src_path} to {link_path}.")
                os.symlink(src_path, link_path)
        return

    async def post_worker_exit(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        worker_id: str,
    ) -> None:
        if not runtime_env.working_dir():
            return
        # TODO(SongGuyang): Cache working dirs for debugging.
        if runtime_env_constants.DISABLE_WORKING_DIR_GC:
            default_logger.info(
                f"Working directory GC has been disabled. The dir {worker_id} "
                "won't be deleted."
            )
            return
        default_logger.info(f"Deleting working dir for worker {worker_id}.")
        working_dir = os.path.join(self._working_dirs, worker_id)
        # TODO(SongGuyang): Use async method to remove, such as aiofiles.os.removedirs
        shutil.rmtree(working_dir)
        return
