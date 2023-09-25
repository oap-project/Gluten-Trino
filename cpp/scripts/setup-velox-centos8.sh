#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -x
set -e
set -o pipefail

PROJECT_DIR="$(cd "`dirname "$0"`"; cd ..; pwd)"
THIRDPARTY_DIR=${PROJECT_DIR}/thirdparty

if [[ ! -d ${THIRDPARTY_DIR} ]]; then
  mkdir -p ${THIRDPARTY_DIR}
fi

source ${PROJECT_DIR}/scripts/setup-helper-functions.sh

BUILD_FLAGS="$(get_cxx_flags)"
export CFLAGS="-fPIC ${BUILD_FLAGS}"
export CXXFLAGS="-fPIC ${BUILD_FLAGS}"
export CPPFLAGS="-fPIC ${BUILD_FLAGS}"
BUILD_TYPE=RelWithDebInfo
JOBS=$(nproc)

function dnf_install {
  dnf install -y -q --setopt=install_weak_deps=False "$@"
}

dnf_install epel-release dnf-plugins-core # For ccache, ninja
dnf config-manager --set-enabled powertools
dnf_install ninja-build ccache gcc-toolset-9 git wget which libevent-devel \
  openssl-devel re2-devel libzstd-devel lz4-devel double-conversion-devel \
  libdwarf-devel curl-devel cmake libicu-devel libuuid-devel libxml2-devel \
  libgsasl-devel

dnf remove -y gflags

# Required for Thrift
dnf_install autoconf automake libtool bison flex python3

# Required for google-cloud-storage
dnf_install curl-devel c-ares-devel

dnf_install conda

# Activate gcc9; enable errors on unset variables afterwards.
source /opt/rh/gcc-toolset-9/enable || exit 1

dnf clean all

FACEBOOK_TOOLCHAIN_VERSION="v2022.11.14.00"

# build lzo 2.10
pushd ${THIRDPARTY_DIR}
wget http://www.oberhumer.com/opensource/lzo/download/lzo-2.10.tar.gz
tar zxf lzo-2.10.tar.gz
cd lzo-2.10
./configure --prefix=/usr --enable-shared --disable-static --docdir=/usr/share/doc/lzo-2.10
make "-j${JOBS}"
make install
popd

# build boost 1.72.0
pushd ${THIRDPARTY_DIR}
wget https://boostorg.jfrog.io/artifactory/main/release/1.72.0/source/boost_1_72_0.tar.gz
tar zxf boost_1_72_0.tar.gz
cd boost_1_72_0
./bootstrap.sh --prefix=/usr/local
./b2 "-j${JOBS}" -d0 install threading=multi
popd

# build gflags 2.2.2
pushd ${THIRDPARTY_DIR}
wget -O gflags-2.2.2.tar.gz https://github.com/gflags/gflags/archive/v2.2.2.tar.gz
tar zxf gflags-2.2.2.tar.gz
mkdir -p gflags-2.2.2/bld && cd gflags-2.2.2/bld
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DBUILD_gflags_LIB=ON -DLIB_SUFFIX=64 -DCMAKE_INSTALL_PREFIX:PATH=/usr -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build glog 0.4.0
pushd ${THIRDPARTY_DIR}
wget -O glog-0.4.0.tar.gz https://github.com/google/glog/archive/v0.4.0.tar.gz
tar zxf glog-0.4.0.tar.gz
mkdir -p glog-0.4.0/bld && cd glog-0.4.0/bld
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX:PATH=/usr -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build snappy 1.1.8
pushd ${THIRDPARTY_DIR}
wget -O snappy-1.1.8.tar.gz https://github.com/google/snappy/archive/1.1.8.tar.gz
tar zxf snappy-1.1.8.tar.gz
mkdir -p snappy-1.1.8/bld && cd snappy-1.1.8/bld
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build fmt 8.0.1
pushd ${THIRDPARTY_DIR}
wget -O fmt-8.0.1.tar.gz https://github.com/fmtlib/fmt/archive/8.0.1.tar.gz
tar zxf fmt-8.0.1.tar.gz
mkdir fmt-8.0.1/bld && cd fmt-8.0.1/bld
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DFMT_TEST=OFF -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build libsodium 1.0.17
pushd ${THIRDPARTY_DIR}
wget -O libsodium-1.0.17.tar.gz "https://github.com/jedisct1/libsodium/releases/download/1.0.17/libsodium-1.0.17.tar.gz"
tar zxf libsodium-1.0.17.tar.gz && cd libsodium-1.0.17
./configure
make -j ${JOBS} install
popd

# build googletest v1.14.0
pushd ${THIRDPARTY_DIR}
git clone https://github.com/google/googletest.git --recursive -b v1.14.0
mkdir googletest/bld && cd googletest/bld
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build protobuf v3.21.12
pushd ${THIRDPARTY_DIR}
git clone https://github.com/protocolbuffers/protobuf.git --recursive -b v3.21.12
mkdir protobuf/bld && cd protobuf/bld
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -Dprotobuf_BUILD_TESTS=OFF -G Ninja
ninja install
popd

# build folly
pushd ${THIRDPARTY_DIR}
git clone https://github.com/facebook/folly.git --recursive -b ${FACEBOOK_TOOLCHAIN_VERSION}
mkdir folly/bld && cd folly/bld
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -DFOLLY_HAVE_INT128_T=1 -G Ninja
ninja install
popd

# build fizz
pushd ${THIRDPARTY_DIR}
git clone https://github.com/facebookincubator/fizz.git --recursive -b ${FACEBOOK_TOOLCHAIN_VERSION}
mkdir fizz/bld && cd fizz/bld
cmake ../fizz -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build wangle
pushd ${THIRDPARTY_DIR}
git clone https://github.com/facebook/wangle.git --recursive -b ${FACEBOOK_TOOLCHAIN_VERSION}
mkdir -p wangle/bld && cd wangle/bld
cmake ../wangle -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build gperf
pushd ${THIRDPARTY_DIR}
wget -O gperf-3.1.tar.gz "http://ftp.gnu.org/pub/gnu/gperf/gperf-3.1.tar.gz"
tar zxf gperf-3.1.tar.gz && cd gperf-3.1
./configure
make -j ${JOBS} install
popd

# build proxygen
pushd ${THIRDPARTY_DIR}
git clone https://github.com/facebook/proxygen.git --recursive -b ${FACEBOOK_TOOLCHAIN_VERSION}
mkdir -p proxygen/bld && cd proxygen/bld
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build antlr4
pushd ${THIRDPARTY_DIR}
wget https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip
mkdir antlr4-cpp-runtime-4.9.3-source && cd antlr4-cpp-runtime-4.9.3-source && unzip ../antlr4-cpp-runtime-4.9.3-source.zip
mkdir bld && cd bld
cmake .. -DANTLR4_INSTALL=ON -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build aws cpp sdk
pushd ${THIRDPARTY_DIR}
git clone https://github.com/aws/aws-sdk-cpp.git --recursive -b 1.11.157
mkdir aws-sdk-cpp/bld && cd aws-sdk-cpp/bld
cmake .. -DENABLE_TESTING=OFF -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja install
popd

# build libhdfs3
pushd ${THIRDPARTY_DIR}
git clone https://github.com/apache/hawq.git --recursive
mkdir -p hawq/depends/libhdfs3/build
mkdir -p hawq/depends/thirdparty/googletest/build && cd hawq/depends/thirdparty/googletest/build
cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -Dgtest_force_shared_crt=ON -DCMAKE_CXX_FLAGS="${CXXFLAGS}" -G Ninja
ninja
pushd ${THIRDPARTY_DIR}/hawq/depends/libhdfs3
sed -i "/-lc++/d" CMake/FindGoogleTest.cmake
sed -i "s/-dumpversion/--version/g" CMake/Platform.cmake
cd build
./../bootstrap --prefix=/usr/local
make -j ${JOBS} install
popd
popd

# install some dependencies for generating protocol files.
pip3 install --upgrade pip # need upgrade before installing, otherwise errors occur here.
pip3 install regex pyyaml chevron clang-format

echo "Every dependencies have been installed in your environment, but you still need to install JDK manually which version must be no less than 17."
