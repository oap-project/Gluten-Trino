rm build -rf
mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=release ..
make -j

# ./build/src/libtrino_bridge.so 
