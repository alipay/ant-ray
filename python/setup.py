from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
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
ray_files_generated = [
    "src/common/thirdparty/redis/src/redis-server",
    "src/common/redis_module/libray_redis_module.so",
    "src/plasma/plasma_store", "src/plasma/plasma_manager",
    "src/local_scheduler/local_scheduler",
    "src/local_scheduler/liblocal_scheduler_library_python.so",
    "src/global_scheduler/global_scheduler", "src/ray/raylet/raylet_monitor",
    "src/ray/raylet/raylet"
]
ray_files = ["ray/WebUI.ipynb"]

# These are the directories where automatically generated Python flatbuffer
# bindings are created.
generated_python_directories = [
    "ray/core/generated", "ray/core/generated/ray",
    "ray/core/generated/ray/protocol"
]

optional_ray_files_generated = []
optional_ray_files = []

ray_ui_files_generated = [
    "src/catapult_files/index.html",
    "src/catapult_files/trace_viewer_full.html"
]

ray_autoscaler_files = ["ray/autoscaler/aws/example-full.yaml"]

if "RAY_USE_NEW_GCS" in os.environ and os.environ["RAY_USE_NEW_GCS"] == "on":
    ray_files_generated += [
        "src/credis/build/src/libmember.so",
        "src/credis/build/src/libmaster.so",
        "src/credis/redis/src/redis-server"
    ]

# The UI files are mandatory if the INCLUDE_UI environment variable equals 1.
# Otherwise, they are optional.
if "INCLUDE_UI" in os.environ and os.environ["INCLUDE_UI"] == "1":
    ray_files_generated += ray_ui_files_generated
else:
    optional_ray_files_generated += ray_ui_files_generated

optional_ray_files += ray_autoscaler_files

extras = {
    "rllib": [
        "tensorflow", "pyyaml", "gym[atari]", "opencv-python", "lz4", "scipy"
    ]
}


class build_ext(_build_ext.build_ext):
    def run(self):
        # Note: We are passing in sys.executable so that we use the same
        # version of Python to build pyarrow inside the build.sh script. Note
        # that certain flags will not be passed along such as --user or sudo.
        # TODO(rkn): Fix this.
        subprocess.check_call(["../build.sh", "-p", sys.executable])

        # Copy over the generated ray files from compilation.
        for filename in ray_files_generated:
            source = "../build/" + filename
            destination = "ray/core/" + filename
            self.move_file(source, os.path.join(self.build_lib, destination))
            self.move_file(source, destination)
        # Copy over the existing ray files.
        for filename in ray_files:
            self.move_file(filename, os.path.join(self.build_lib, filename))

        # We also need to install pyarrow along with Ray, so make sure that the
        # relevant non-Python pyarrow files get copied.
        pyarrow_files = [
            os.path.join("ray/pyarrow_files/pyarrow", filename)
            for filename in os.listdir("./ray/pyarrow_files/pyarrow")
            if not os.path.isdir(
                os.path.join("ray/pyarrow_files/pyarrow", filename))
        ]

        other_files_to_include = pyarrow_files

        # Copy over the autogenerated flatbuffer Python bindings.
        for directory in generated_python_directories:
            for filename in os.listdir(directory):
                if filename[-3:] == ".py":
                    other_files_to_include.append(
                        os.path.join(directory, filename))

        for filename in other_files_to_include:
            self.move_file(filename, os.path.join(self.build_lib, filename))

        # Try to copy over the generated optional files.
        for filename in optional_ray_files_generated:
            try:
                source = "../build/" + filename
                destination = "ray/core/" + filename
                self.move_file(source, os.path.join(self.build_lib,
                                                    destination))
                self.move_file(source, destination)
            except Exception as e:
                print("Failed to copy optional file {}. This is ok."
                      .format(source))
        # Try to copy over the existing optional files.
        for filename in optional_ray_files:
            try:
                self.move_file(filename, os.path.join(self.build_lib,
                                                      filename))
            except Exception as e:
                print("Failed to copy optional file {}. This is ok."
                      .format(filename))

    def move_file(self, source, destination):
        # TODO(rkn): This feels very brittle. It may not handle all cases. See
        # https://github.com/apache/arrow/blob/master/python/setup.py for an
        # example.
        # Create the target directory if it doesn't already exist.
        parent_directory = os.path.dirname(destination)
        if not os.path.exists(parent_directory):
            os.makedirs(parent_directory)
        print("Copying {} to {}.".format(source, destination))
        shutil.copy(source, destination)


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


setup(
    name="ray",
    # The version string is also in __init__.py. TODO(pcm): Fix this.
    version="0.4.0",
    packages=find_packages(),
    cmdclass={"build_ext": build_ext},
    # The BinaryDistribution argument triggers build_ext.
    distclass=BinaryDistribution,
    install_requires=[
        "numpy",
        "funcsigs",
        "click",
        "colorama",
        "psutil",
        "pytest",
        "pyyaml",
        "redis",
        # The six module is required by pyarrow.
        "six >= 1.0.0",
        "flatbuffers"
    ],
    setup_requires=["cython >= 0.23"],
    extras_require=extras,
    entry_points={"console_scripts": ["ray=ray.scripts.scripts:main"]},
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0")
