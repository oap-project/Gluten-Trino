SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
source $SCRIPTDIR/setup-velox-ubuntu.sh
sudo apt install -y gperf uuid-dev libsodium-dev

function install_proxygen {
  github_checkout facebook/proxygen "${FB_OS_VERSION}"
  cmake_install -DBUILD_TESTS=OFF
}

function install_antlr4 {
	wget https://www.antlr.org/download/antlr4-cpp-runtime-4.9.3-source.zip -O antlr4-cpp-runtime-4.9.3-source.zip
	mkdir antlr4-cpp-runtime-4.9.3-source
	pushd antlr4-cpp-runtime-4.9.3-source
	unzip ../antlr4-cpp-runtime-4.9.3-source.zip
	mkdir build && cd build
	cmake .. -DANTLR4_INSTALL=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON
  make "-j${NPROC}" 
  make install
  popd
}

function install_trino_deps {
  install_velox_deps
  run_and_time install_proxygen
  run_and_time install_antlr4
}

if [[ $# -ne 0 ]]; then
  for cmd in "$@"; do
    run_and_time "${cmd}"
  done
else
  install_trino_deps
fi