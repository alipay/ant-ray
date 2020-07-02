from itertools import chain
import os
import re
import shutil
import subprocess
import sys

from setuptools import setup, find_packages, Distribution
import setuptools.command.build_ext as _build_ext

# Ideally, we could include these files by putting them in a
# MANIFEST.in or using the package_data argument to setup, but the
# MANIFEST.in gets applied at the very beginning when setup.py runs
# before these files have been created, so we have to move the files
# manually.

exe_suffix = ".exe" if sys.platform == "win32" else ""

# .pyd is the extension Python requires on Windows for shared libraries.
# https://docs.python.org/3/faq/windows.html#is-a-pyd-file-the-same-as-a-dll
pyd_suffix = ".pyd" if sys.platform == "win32" else ".so"

# NOTE: The lists below must be kept in sync with ray/BUILD.bazel.
ray_files = [
    "ray/core/src/ray/thirdparty/redis/src/redis-server",
    "ray/core/src/ray/gcs/redis_module/libray_redis_module.so",
    "ray/core/src/plasma/plasma_store_server" + exe_suffix,
    "ray/_raylet" + pyd_suffix,
    "ray/core/src/ray/gcs/gcs_server" + exe_suffix,
    "ray/core/src/ray/raylet/raylet" + exe_suffix,
    "ray/streaming/_streaming.so",
]

build_java = os.getenv("RAY_INSTALL_JAVA") == "1"
if build_java:
    ray_files.append("ray/jars/ray_dist.jar")

# These are the directories where automatically generated Python protobuf
# bindings are created.
generated_python_directories = [
    "ray/core/generated",
    "ray/streaming/generated",
]

optional_ray_files = []

ray_autoscaler_files = [
    "ray/autoscaler/aws/example-full.yaml",
    "ray/autoscaler/azure/example-full.yaml",
    "ray/autoscaler/azure/azure-vm-template.json",
    "ray/autoscaler/azure/azure-config-template.json",
    "ray/autoscaler/gcp/example-full.yaml",
    "ray/autoscaler/local/example-full.yaml",
    "ray/autoscaler/kubernetes/example-full.yaml",
    "ray/autoscaler/kubernetes/kubectl-rsync.sh",
    "ray/autoscaler/ray-schema.json"
]

ray_project_files = [
    "ray/projects/schema.json", "ray/projects/templates/cluster_template.yaml",
    "ray/projects/templates/project_template.yaml",
    "ray/projects/templates/requirements.txt"
]

ray_dashboard_files = [
    os.path.join(dirpath, filename)
    for dirpath, dirnames, filenames in os.walk("ray/dashboard/client/build")
    for filename in filenames
]

optional_ray_files += ray_autoscaler_files
optional_ray_files += ray_project_files
optional_ray_files += ray_dashboard_files

if "RAY_USE_NEW_GCS" in os.environ and os.environ["RAY_USE_NEW_GCS"] == "on":
    ray_files += [
        "ray/core/src/credis/build/src/libmember.so",
        "ray/core/src/credis/build/src/libmaster.so",
        "ray/core/src/credis/redis/src/redis-server"
    ]

extras = {
    "debug": [],
    "dashboard": ["requests", "gpustat"],
    "serve": ["uvicorn", "flask", "blist", "requests"],
    "tune": ["tabulate", "tensorboardX", "pandas"]
}

extras["rllib"] = extras["tune"] + [
    "atari_py",
    "dm_tree",
    "gym[atari]",
    "lz4",
    "opencv-python-headless",
    "pyyaml",
    "scipy",
]

extras["streaming"] = ["msgpack >= 0.6.2"]

extras["all"] = list(set(chain.from_iterable(extras.values())))


class build_ext(_build_ext.build_ext):
    def run(self):
        # Note: We are passing in sys.executable so that we use the same
        # version of Python to build packages inside the build.sh script. Note
        # that certain flags will not be passed along such as --user or sudo.
        # TODO(rkn): Fix this.
        command = ["../build.sh", "-p", sys.executable]
        if sys.platform == "win32" and command[0].lower().endswith(".sh"):
            # We can't run .sh files directly in Windows, so find a shell.
            # Don't use "bash" instead of "sh", because that might run the Bash
            # from WSL! (We want MSYS2's Bash, which is also sh by default.)
            shell = os.getenv("BAZEL_SH", "sh")  # NOT "bash"! (see above)
            command.insert(0, shell)
        if build_java:
            # Also build binaries for Java if the above env variable exists.
            command += ["-l", "python,java"]
        subprocess.check_call(command)

        # We also need to install pickle5 along with Ray, so make sure that the
        # relevant non-Python pickle5 files get copied.
        pickle5_files = self.walk_directory("./ray/pickle5_files/pickle5")

        thirdparty_files = self.walk_directory("./ray/thirdparty_files")

        files_to_include = ray_files + pickle5_files + thirdparty_files

        # Copy over the autogenerated protobuf Python bindings.
        for directory in generated_python_directories:
            for filename in os.listdir(directory):
                if filename[-3:] == ".py":
                    files_to_include.append(os.path.join(directory, filename))

        for filename in files_to_include:
            self.move_file(filename)

        # Try to copy over the optional files.
        for filename in optional_ray_files:
            try:
                self.move_file(filename)
            except Exception:
                print("Failed to copy optional file {}. This is ok."
                      .format(filename))

    def walk_directory(self, directory):
        file_list = []
        for (root, dirs, filenames) in os.walk(directory):
            for name in filenames:
                file_list.append(os.path.join(root, name))
        return file_list

    def move_file(self, filename):
        # TODO(rkn): This feels very brittle. It may not handle all cases. See
        # https://github.com/apache/arrow/blob/master/python/setup.py for an
        # example.
        source = filename
        destination = os.path.join(self.build_lib, filename)
        # Create the target directory if it doesn't already exist.
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        if not os.path.exists(destination):
            print("Copying {} to {}.".format(source, destination))
            if sys.platform == "win32":
                # Does not preserve file mode (needed to avoid read-only bit)
                shutil.copyfile(source, destination, follow_symlinks=True)
            else:
                # Preserves file mode (needed to copy executable bit)
                shutil.copy(source, destination, follow_symlinks=True)


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


def find_version(*filepath):
    # Extract version information from filepath
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, *filepath)) as fp:
        version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                                  fp.read(), re.M)
        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


requires = [
    "aiohttp",
    "click >= 7.0",
    "colorama",
    "filelock",
    "google",
    "grpcio",
    "jsonschema",
    "msgpack >= 0.6.0, < 2.0.0",
    "numpy >= 1.16",
    "protobuf >= 3.8.0",
    "py-spy >= 0.2.0",
    "pyyaml",
    "redis >= 3.3.2, < 3.5.0",
]

setup(
    name="ray",
    version=find_version("ray", "__init__.py"),
    author="Ray Team",
    author_email="ray-dev@googlegroups.com",
    description=("A system for parallel and distributed Python that unifies "
                 "the ML ecosystem."),
    long_description=open("../README.rst").read(),
    url="https://github.com/ray-project/ray",
    keywords=("ray distributed parallel machine-learning "
              "reinforcement-learning deep-learning python"),
    packages=find_packages(),
    cmdclass={"build_ext": build_ext},
    # The BinaryDistribution argument triggers build_ext.
    distclass=BinaryDistribution,
    install_requires=requires,
    setup_requires=["cython >= 0.29.14", "wheel"],
    extras_require=extras,
    entry_points={
        "console_scripts": [
            "ray=ray.scripts.scripts:main",
            "rllib=ray.rllib.scripts:cli [rllib]", "tune=ray.tune.scripts:cli"
        ]
    },
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0")
