import binascii
import errno
import hashlib
import inspect
import logging
import numpy as np
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid

import ray
import ray.gcs_utils
import ray.ray_constants as ray_constants
import psutil

logger = logging.getLogger(__name__)

# Linux can bind child processes' lifetimes to that of their parents via prctl.
# prctl support is detected dynamically once, and assumed thereafter.
linux_prctl = None

# Windows can bind processes' lifetimes to that of kernel-level "job objects".
# We keep a global job object to tie its lifetime to that of our own process.
win32_job = None
win32_AssignProcessToJobObject = None


def get_user_temp_dir():
    if sys.platform.startswith("darwin") or sys.platform.startswith("linux"):
        # Ideally we wouldn't need this fallback, but keep it for now for
        # for compatibility
        tempdir = os.path.join(os.sep, "tmp")
    else:
        tempdir = tempfile.gettempdir()
    return tempdir


def get_ray_temp_dir():
    return os.path.join(get_user_temp_dir(), "ray")


def _random_string():
    id_hash = hashlib.sha1()
    id_hash.update(uuid.uuid4().bytes)
    id_bytes = id_hash.digest()
    assert len(id_bytes) == ray_constants.ID_SIZE
    return id_bytes


def format_error_message(exception_message, task_exception=False):
    """Improve the formatting of an exception thrown by a remote function.

    This method takes a traceback from an exception and makes it nicer by
    removing a few uninformative lines and adding some space to indent the
    remaining lines nicely.

    Args:
        exception_message (str): A message generated by traceback.format_exc().

    Returns:
        A string of the formatted exception message.
    """
    lines = exception_message.split("\n")
    if task_exception:
        # For errors that occur inside of tasks, remove lines 1 and 2 which are
        # always the same, they just contain information about the worker code.
        lines = lines[0:1] + lines[3:]
        pass
    return "\n".join(lines)


def push_error_to_driver(worker, error_type, message, job_id=None):
    """Push an error message to the driver to be printed in the background.

    Args:
        worker: The worker to use.
        error_type (str): The type of the error.
        message (str): The message that will be printed in the background
            on the driver.
        job_id: The ID of the driver to push the error message to. If this
            is None, then the message will be pushed to all drivers.
    """
    if job_id is None:
        job_id = ray.JobID.nil()
    assert isinstance(job_id, ray.JobID)
    worker.core_worker.push_error(job_id, error_type, message, time.time())


def push_error_to_driver_through_redis(redis_client,
                                       error_type,
                                       message,
                                       job_id=None):
    """Push an error message to the driver to be printed in the background.

    Normally the push_error_to_driver function should be used. However, in some
    instances, the raylet client is not available, e.g., because the
    error happens in Python before the driver or worker has connected to the
    backend processes.

    Args:
        redis_client: The redis client to use.
        error_type (str): The type of the error.
        message (str): The message that will be printed in the background
            on the driver.
        job_id: The ID of the driver to push the error message to. If this
            is None, then the message will be pushed to all drivers.
    """
    if job_id is None:
        job_id = ray.JobID.nil()
    assert isinstance(job_id, ray.JobID)
    # Do everything in Python and through the Python Redis client instead
    # of through the raylet.
    error_data = ray.gcs_utils.construct_error_message(job_id, error_type,
                                                       message, time.time())
    redis_client.publish("ERROR_INFO:" + job_id.hex(), error_data)


def is_cython(obj):
    """Check if an object is a Cython function or method"""

    # TODO(suo): We could split these into two functions, one for Cython
    # functions and another for Cython methods.
    # TODO(suo): There doesn't appear to be a Cython function 'type' we can
    # check against via isinstance. Please correct me if I'm wrong.
    def check_cython(x):
        return type(x).__name__ == "cython_function_or_method"

    # Check if function or method, respectively
    return check_cython(obj) or \
        (hasattr(obj, "__func__") and check_cython(obj.__func__))


def is_function_or_method(obj):
    """Check if an object is a function or method.

    Args:
        obj: The Python object in question.

    Returns:
        True if the object is an function or method.
    """
    return inspect.isfunction(obj) or inspect.ismethod(obj) or is_cython(obj)


def is_class_method(f):
    """Returns whether the given method is a class_method."""
    return hasattr(f, "__self__") and f.__self__ is not None


def is_static_method(cls, f_name):
    """Returns whether the class has a static method with the given name.

    Args:
        cls: The Python class (i.e. object of type `type`) to
            search for the method in.
        f_name: The name of the method to look up in this class
            and check whether or not it is static.
    """
    for cls in inspect.getmro(cls):
        if f_name in cls.__dict__:
            return isinstance(cls.__dict__[f_name], staticmethod)
    return False


def random_string():
    """Generate a random string to use as an ID.

    Note that users may seed numpy, which could cause this function to generate
    duplicate IDs. Therefore, we need to seed numpy ourselves, but we can't
    interfere with the state of the user's random number generator, so we
    extract the state of the random number generator and reset it after we are
    done.

    TODO(rkn): If we want to later guarantee that these are generated in a
    deterministic manner, then we will need to make some changes here.

    Returns:
        A random byte string of length ray_constants.ID_SIZE.
    """
    # Get the state of the numpy random number generator.
    numpy_state = np.random.get_state()
    # Try to use true randomness.
    np.random.seed(None)
    # Generate the random ID.
    random_id = np.random.bytes(ray_constants.ID_SIZE)
    # Reset the state of the numpy random number generator.
    np.random.set_state(numpy_state)
    return random_id


def decode(byte_str, allow_none=False):
    """Make this unicode in Python 3, otherwise leave it as bytes.

    Args:
        byte_str: The byte string to decode.
        allow_none: If true, then we will allow byte_str to be None in which
            case we will return an empty string. TODO(rkn): Remove this flag.
            This is only here to simplify upgrading to flatbuffers 1.10.0.

    Returns:
        A byte string in Python 2 and a unicode string in Python 3.
    """
    if byte_str is None and allow_none:
        return ""

    if not isinstance(byte_str, bytes):
        raise ValueError(
            "The argument {} must be a bytes object.".format(byte_str))
    if sys.version_info >= (3, 0):
        return byte_str.decode("ascii")
    else:
        return byte_str


def ensure_str(s, encoding="utf-8", errors="strict"):
    """Coerce *s* to `str`.

      - `str` -> `str`
      - `bytes` -> decoded to `str`
    """
    if isinstance(s, str):
        return s
    else:
        assert isinstance(s, bytes)
        return s.decode(encoding, errors)


def binary_to_object_ref(binary_object_ref):
    return ray.ObjectRef(binary_object_ref)


def binary_to_task_id(binary_task_id):
    return ray.TaskID(binary_task_id)


def binary_to_hex(identifier):
    hex_identifier = binascii.hexlify(identifier)
    if sys.version_info >= (3, 0):
        hex_identifier = hex_identifier.decode()
    return hex_identifier


def hex_to_binary(hex_identifier):
    return binascii.unhexlify(hex_identifier)


# TODO(qwang): Remove these hepler functions
# once we separate `WorkerID` from `UniqueID`.
def compute_job_id_from_driver(driver_id):
    assert isinstance(driver_id, ray.WorkerID)
    return ray.JobID(driver_id.binary()[0:ray.JobID.size()])


def compute_driver_id_from_job(job_id):
    assert isinstance(job_id, ray.JobID)
    rest_length = ray_constants.ID_SIZE - job_id.size()
    driver_id_str = job_id.binary() + (rest_length * b"\xff")
    return ray.WorkerID(driver_id_str)


def get_cuda_visible_devices():
    """Get the device IDs in the CUDA_VISIBLE_DEVICES environment variable.

    Returns:
        if CUDA_VISIBLE_DEVICES is set, this returns a list of integers with
            the IDs of the GPUs. If it is not set or is set to NoDevFiles,
            this returns None.
    """
    gpu_ids_str = os.environ.get("CUDA_VISIBLE_DEVICES", None)

    if gpu_ids_str is None:
        return None

    if gpu_ids_str == "":
        return []

    if gpu_ids_str == "NoDevFiles":
        return []

    return [int(i) for i in gpu_ids_str.split(",")]


last_set_gpu_ids = None


def set_cuda_visible_devices(gpu_ids):
    """Set the CUDA_VISIBLE_DEVICES environment variable.

    Args:
        gpu_ids: This is a list of integers representing GPU IDs.
    """

    global last_set_gpu_ids
    if last_set_gpu_ids == gpu_ids:
        return  # optimization: already set

    os.environ["CUDA_VISIBLE_DEVICES"] = ",".join([str(i) for i in gpu_ids])
    last_set_gpu_ids = gpu_ids


def resources_from_resource_arguments(
        default_num_cpus, default_num_gpus, default_memory,
        default_object_store_memory, default_resources, runtime_num_cpus,
        runtime_num_gpus, runtime_memory, runtime_object_store_memory,
        runtime_resources):
    """Determine a task's resource requirements.

    Args:
        default_num_cpus: The default number of CPUs required by this function
            or actor method.
        default_num_gpus: The default number of GPUs required by this function
            or actor method.
        default_memory: The default heap memory required by this function
            or actor method.
        default_object_store_memory: The default object store memory required
            by this function or actor method.
        default_resources: The default custom resources required by this
            function or actor method.
        runtime_num_cpus: The number of CPUs requested when the task was
            invoked.
        runtime_num_gpus: The number of GPUs requested when the task was
            invoked.
        runtime_memory: The heap memory requested when the task was invoked.
        runtime_object_store_memory: The object store memory requested when
            the task was invoked.
        runtime_resources: The custom resources requested when the task was
            invoked.

    Returns:
        A dictionary of the resource requirements for the task.
    """
    if runtime_resources is not None:
        resources = runtime_resources.copy()
    elif default_resources is not None:
        resources = default_resources.copy()
    else:
        resources = {}

    if "CPU" in resources or "GPU" in resources:
        raise ValueError("The resources dictionary must not "
                         "contain the key 'CPU' or 'GPU'")
    elif "memory" in resources or "object_store_memory" in resources:
        raise ValueError("The resources dictionary must not "
                         "contain the key 'memory' or 'object_store_memory'")

    assert default_num_cpus is not None
    resources["CPU"] = (default_num_cpus
                        if runtime_num_cpus is None else runtime_num_cpus)

    if runtime_num_gpus is not None:
        resources["GPU"] = runtime_num_gpus
    elif default_num_gpus is not None:
        resources["GPU"] = default_num_gpus

    memory = default_memory or runtime_memory
    object_store_memory = (default_object_store_memory
                           or runtime_object_store_memory)
    if memory is not None:
        resources["memory"] = ray_constants.to_memory_units(
            memory, round_up=True)
    if object_store_memory is not None:
        resources["object_store_memory"] = ray_constants.to_memory_units(
            object_store_memory, round_up=True)

    return resources


_default_handler = None


def setup_logger(logging_level, logging_format):
    """Setup default logging for ray."""
    logger = logging.getLogger("ray")
    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    logger.setLevel(logging_level)
    global _default_handler
    if _default_handler is None:
        _default_handler = logging.StreamHandler()
        logger.addHandler(_default_handler)
    _default_handler.setFormatter(logging.Formatter(logging_format))
    logger.propagate = False


def open_log(path, **kwargs):
    """
    Opens the log file at `path`, with the provided kwargs being given to
    `open`.
    """
    kwargs.setdefault("buffering", 1)
    kwargs.setdefault("mode", "a")
    kwargs.setdefault("encoding", "utf-8")
    return open(path, **kwargs)


def create_and_init_new_worker_log(path, worker_pid):
    """
    Opens a path (or creates if necessary) for a log.

    Args:
        path (str): The name/path of the file to be opened.
        worker_pid (int): The pid of the worker process.

    Returns:
        A file-like object which can be written to.
    """
    # TODO (Alex): We should eventually be able to replace this with
    # named-pipes.
    f = open_log(path)
    # Check to see if we're creating this file. No one else should ever write
    # to this file, so we don't have to worry about TOCTOU.
    if f.tell() == 0:
        # This should always be the first message to appear in the worker's
        # stdout and stderr log files. The string "Ray worker pid:" is
        # parsed in the log monitor process.
        print("Ray worker pid: {}".format(worker_pid), file=f)
    return f


def get_system_memory():
    """Return the total amount of system memory in bytes.

    Returns:
        The total amount of system memory in bytes.
    """
    # Try to accurately figure out the memory limit if we are in a docker
    # container. Note that this file is not specific to Docker and its value is
    # often much larger than the actual amount of memory.
    docker_limit = None
    memory_limit_filename = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
    if os.path.exists(memory_limit_filename):
        with open(memory_limit_filename, "r") as f:
            docker_limit = int(f.read())

    # Use psutil if it is available.
    psutil_memory_in_bytes = psutil.virtual_memory().total

    if docker_limit is not None:
        # We take the min because the cgroup limit is very large if we aren't
        # in Docker.
        return min(docker_limit, psutil_memory_in_bytes)

    return psutil_memory_in_bytes


def get_used_memory():
    """Return the currently used system memory in bytes

    Returns:
        The total amount of used memory
    """
    # Try to accurately figure out the memory usage if we are in a docker
    # container.
    docker_usage = None
    memory_usage_filename = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
    if os.path.exists(memory_usage_filename):
        with open(memory_usage_filename, "r") as f:
            docker_usage = int(f.read())

    # Use psutil if it is available.
    psutil_memory_in_bytes = psutil.virtual_memory().used

    if docker_usage is not None:
        # We take the min because the cgroup limit is very large if we aren't
        # in Docker.
        return min(docker_usage, psutil_memory_in_bytes)

    return psutil_memory_in_bytes


def estimate_available_memory():
    """Return the currently available amount of system memory in bytes.

    Returns:
        The total amount of available memory in bytes. Based on the used
        and total memory.

    """
    return get_system_memory() - get_used_memory()


def get_shared_memory_bytes():
    """Get the size of the shared memory file system.

    Returns:
        The size of the shared memory file system in bytes.
    """
    # Make sure this is only called on Linux.
    assert sys.platform == "linux" or sys.platform == "linux2"

    shm_fd = os.open("/dev/shm", os.O_RDONLY)
    try:
        shm_fs_stats = os.fstatvfs(shm_fd)
        # The value shm_fs_stats.f_bsize is the block size and the
        # value shm_fs_stats.f_bavail is the number of available
        # blocks.
        shm_avail = shm_fs_stats.f_bsize * shm_fs_stats.f_bavail
    finally:
        os.close(shm_fd)

    return shm_avail


def check_oversized_pickle(pickled, name, obj_type, worker):
    """Send a warning message if the pickled object is too large.

    Args:
        pickled: the pickled object.
        name: name of the pickled object.
        obj_type: type of the pickled object, can be 'function',
            'remote function', 'actor', or 'object'.
        worker: the worker used to send warning message.
    """
    length = len(pickled)
    if length <= ray_constants.PICKLE_OBJECT_WARNING_SIZE:
        return
    warning_message = (
        "Warning: The {} {} has size {} when pickled. "
        "It will be stored in Redis, which could cause memory issues. "
        "This may mean that its definition uses a large array or other object."
    ).format(obj_type, name, length)
    push_error_to_driver(
        worker,
        ray_constants.PICKLING_LARGE_OBJECT_PUSH_ERROR,
        warning_message,
        job_id=worker.current_job_id)


def is_main_thread():
    return threading.current_thread().getName() == "MainThread"


def detect_fate_sharing_support_win32():
    global win32_job, win32_AssignProcessToJobObject
    if win32_job is None and sys.platform == "win32":
        import ctypes
        try:
            from ctypes.wintypes import BOOL, DWORD, HANDLE, LPVOID, LPCWSTR
            kernel32 = ctypes.WinDLL("kernel32")
            kernel32.CreateJobObjectW.argtypes = (LPVOID, LPCWSTR)
            kernel32.CreateJobObjectW.restype = HANDLE
            sijo_argtypes = (HANDLE, ctypes.c_int, LPVOID, DWORD)
            kernel32.SetInformationJobObject.argtypes = sijo_argtypes
            kernel32.SetInformationJobObject.restype = BOOL
            kernel32.AssignProcessToJobObject.argtypes = (HANDLE, HANDLE)
            kernel32.AssignProcessToJobObject.restype = BOOL
            kernel32.IsDebuggerPresent.argtypes = ()
            kernel32.IsDebuggerPresent.restype = BOOL
        except (AttributeError, TypeError, ImportError):
            kernel32 = None
        job = kernel32.CreateJobObjectW(None, None) if kernel32 else None
        job = subprocess.Handle(job) if job else job
        if job:
            from ctypes.wintypes import DWORD, LARGE_INTEGER, ULARGE_INTEGER

            class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
                _fields_ = [
                    ("PerProcessUserTimeLimit", LARGE_INTEGER),
                    ("PerJobUserTimeLimit", LARGE_INTEGER),
                    ("LimitFlags", DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", DWORD),
                    ("SchedulingClass", DWORD),
                ]

            class IO_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("ReadOperationCount", ULARGE_INTEGER),
                    ("WriteOperationCount", ULARGE_INTEGER),
                    ("OtherOperationCount", ULARGE_INTEGER),
                    ("ReadTransferCount", ULARGE_INTEGER),
                    ("WriteTransferCount", ULARGE_INTEGER),
                    ("OtherTransferCount", ULARGE_INTEGER),
                ]

            class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
                _fields_ = [
                    ("BasicLimitInformation",
                     JOBOBJECT_BASIC_LIMIT_INFORMATION),
                    ("IoInfo", IO_COUNTERS),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t),
                ]

            debug = kernel32.IsDebuggerPresent()

            # Defined in <WinNT.h>; also available here:
            # https://docs.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-setinformationjobobject
            JobObjectExtendedLimitInformation = 9
            JOB_OBJECT_LIMIT_BREAKAWAY_OK = 0x00000800
            JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION = 0x00000400
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
            buf = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
            buf.BasicLimitInformation.LimitFlags = (
                (0 if debug else JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
                | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION
                | JOB_OBJECT_LIMIT_BREAKAWAY_OK)
            infoclass = JobObjectExtendedLimitInformation
            if not kernel32.SetInformationJobObject(
                    job, infoclass, ctypes.byref(buf), ctypes.sizeof(buf)):
                job = None
        win32_AssignProcessToJobObject = (kernel32.AssignProcessToJobObject
                                          if kernel32 is not None else False)
        win32_job = job if job else False
    return bool(win32_job)


def detect_fate_sharing_support_linux():
    global linux_prctl
    if linux_prctl is None and sys.platform.startswith("linux"):
        try:
            from ctypes import c_int, c_ulong, CDLL
            prctl = CDLL(None).prctl
            prctl.restype = c_int
            prctl.argtypes = [c_int, c_ulong, c_ulong, c_ulong, c_ulong]
        except (AttributeError, TypeError):
            prctl = None
        linux_prctl = prctl if prctl else False
    return bool(linux_prctl)


def detect_fate_sharing_support():
    result = None
    if sys.platform == "win32":
        result = detect_fate_sharing_support_win32()
    elif sys.platform.startswith("linux"):
        result = detect_fate_sharing_support_linux()
    return result


def set_kill_on_parent_death_linux():
    """Ensures this process dies if its parent dies (fate-sharing).

    Linux-only. Must be called in preexec_fn (i.e. by the child).
    """
    if detect_fate_sharing_support_linux():
        import signal
        PR_SET_PDEATHSIG = 1
        if linux_prctl(PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
            import ctypes
            raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
    else:
        assert False, "PR_SET_PDEATHSIG used despite being unavailable"


def set_kill_child_on_death_win32(child_proc):
    """Ensures the child process dies if this process dies (fate-sharing).

    Windows-only. Must be called by the parent, after spawning the child.

    Args:
        child_proc: The subprocess.Popen or subprocess.Handle object.
    """

    if isinstance(child_proc, subprocess.Popen):
        child_proc = child_proc._handle
    assert isinstance(child_proc, subprocess.Handle)

    if detect_fate_sharing_support_win32():
        if not win32_AssignProcessToJobObject(win32_job, int(child_proc)):
            import ctypes
            raise OSError(ctypes.get_last_error(),
                          "AssignProcessToJobObject() failed")
    else:
        assert False, "AssignProcessToJobObject used despite being unavailable"


def set_sigterm_handler(sigterm_handler):
    """Registers a handler for SIGTERM in a platform-compatible manner."""
    if sys.platform == "win32":
        # Note that these signal handlers only work for console applications.
        # TODO(mehrdadn): implement graceful process termination mechanism
        # SIGINT is Ctrl+C, SIGBREAK is Ctrl+Break.
        signal.signal(signal.SIGBREAK, sigterm_handler)
    else:
        signal.signal(signal.SIGTERM, sigterm_handler)


def try_make_directory_shared(directory_path):
    try:
        os.chmod(directory_path, 0o0777)
    except OSError as e:
        # Silently suppress the PermissionError that is thrown by the chmod.
        # This is done because the user attempting to change the permissions
        # on a directory may not own it. The chmod is attempted whether the
        # directory is new or not to avoid race conditions.
        # ray-project/ray/#3591
        if e.errno in [errno.EACCES, errno.EPERM]:
            pass
        else:
            raise


def try_to_create_directory(directory_path):
    """Attempt to create a directory that is globally readable/writable.

    Args:
        directory_path: The path of the directory to create.
    """
    directory_path = os.path.expanduser(directory_path)
    os.makedirs(directory_path, exist_ok=True)
    # Change the log directory permissions so others can use it. This is
    # important when multiple people are using the same machine.
    try_make_directory_shared(directory_path)


def try_to_symlink(symlink_path, target_path):
    """Attempt to create a symlink.

    If the symlink path exists and isn't a symlink, the symlink will not be
    created. If a symlink exists in the path, it will be attempted to be
    removed and replaced.

    Args:
        symlink_path: The path at which to create the symlink.
        target_path: The path the symlink should point to.
    """
    symlink_path = os.path.expanduser(symlink_path)
    target_path = os.path.expanduser(target_path)

    if os.path.exists(symlink_path):
        if os.path.islink(symlink_path):
            # Try to remove existing symlink.
            try:
                os.remove(symlink_path)
            except OSError:
                return
        else:
            # There's an existing non-symlink file, don't overwrite it.
            return

    try:
        os.symlink(target_path, symlink_path)
    except OSError:
        return
