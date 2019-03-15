#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd $ROOT_DIR/..
bazel build -c opt //java:all
pushd $ROOT_DIR/../java/test
# test raylet
java -jar -Dray.home=$ROOT_DIR/../ $ROOT_DIR/../bazel-bin/java/AllTests_deploy.jar $ROOT_DIR/../java/testng.xml

# test raylet under SINGLE_PROCESS mode
java -jar -Dray.home=$ROOT_DIR/../ -Dray.run-mode=SINGLE_PROCESS $ROOT_DIR/../bazel-bin/java/AllTests_deploy.jar $ROOT_DIR/../java/testng.xml

popd
