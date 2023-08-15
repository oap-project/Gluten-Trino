# Enable AddressSanitizer

## Prepare

- Set ```/proc/sys/vm/overcommit_memory``` as ```1```.
- Find ```path/of/libasan.so``` by ```ldd libtrino_bridge.so```.
- Find ```path/of/libstdc++.so``` by ```ldd libtrino_bridge.so```.
- Set environment parameters to JVM: ```LD_PRELOAD=path/of/libasan.so:path/of/libstdc++.so;ASAN_OPTIONS=detect_leaks=1:handle_segv=0;LSAN_OPTIONS=suppressions=./trino-cpp/leak_suppress.txt```