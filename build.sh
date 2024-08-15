#! /bin/bash

# After a completed build, you can use the following command to skip building arrow/velox.
# ./build.sh --enable_ep_cache=ON --build_arrow=OFF

set -exu

PROJECT_DIR="$(cd "$(dirname $0)"; pwd)"
VELOX_REPO="https://private:5e2YGwdoruE9j30GRyRh@git.woa.com/wxg-bigdata/spark/velox.git"

CPP_BUILD_CMD="./dev/builddeps-veloxbe.sh --velox_repo=${VELOX_REPO} --build_jemalloc=ON --enable_vcpkg=ON --enable_hdfs=ON $@"
JAVA_BUILD_CMD="mvn clean package -Pbackends-velox -Pspark-3.3 -Piceberg -DskipTests"

DOCKER_IMG="gluten-velox-dev"

docker build \
  --network=host \
  -t ${DOCKER_IMG} \
  -f ${PROJECT_DIR}/dev/docker/Dockerfile .

docker run -it --rm \
  --network=host \
  -v ${PROJECT_DIR}:${PROJECT_DIR} \
  -v ${PROJECT_DIR}/ep/_ep/install:/usr/local \
  ${DOCKER_IMG} \
  sh -c \
  "cd ${PROJECT_DIR} && \
  export MAVEN_OPTS=-Dmaven.repo.local=${PROJECT_DIR}/.mvn-build && \
  source /opt/rh/gcc-toolset-9/enable && \
  ${CPP_BUILD_CMD} && \
  ${JAVA_BUILD_CMD}"
