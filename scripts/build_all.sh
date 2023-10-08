#!/bin/bash

set -x
set -e
set -o pipefail

if [[ -f /opt/rh/gcc-toolset-9/enable ]]; then
  # It's expected to use gcc 9 to build GlutenTrino.
  source /opt/rh/gcc-toolset-9/enable
fi

GLUTEN_TRINO_HOME="$(cd "`dirname "$0"`"; cd ..; pwd)"

if ! git -v > /dev/null ; then
  dnf install -y git
fi

pushd ${GLUTEN_TRINO_HOME}
git clone https://github.com/trinodb/trino.git -b 411 --depth=1
export TRINO_HOME=${GLUTEN_TRINO_HOME}/trino
popd

# build cpp
cd ${GLUTEN_TRINO_HOME}/cpp
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -G Ninja
ninja

# build java
cd ${GLUTEN_TRINO_HOME}/java
mvn clean package -DskipTests=True -Dair.check.skip-duplicate-finder=True

# make distribution
pushd ${GLUTEN_TRINO_HOME}
mkdir release && cd release
cp ${GLUTEN_TRINO_HOME}/cpp/build/src/libgluten_trino.so .
cp ${GLUTEN_TRINO_HOME}/java/target/Gluten-Trino-411.jar .
popd
