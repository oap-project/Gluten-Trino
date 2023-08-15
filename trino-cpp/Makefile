JOBS?=$(bash getconf _NPROCESSORS_CONF)

.PHONY: depends sanitizer debug release clean

clean:
	@ls | grep '^build-\(Debug\|Release\)' | xargs -r rm -r

build:
	$(MAKE) velox-patch
	@mkdir -p build-$(BUILD_TYPE) && \
	cmake -B build-$(BUILD_TYPE) -GNinja -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DTRINO_BRIDGE_ENABLE_SANITIZER=$(ENABLE_SANITIZER)
	cmake --build build-$(BUILD_TYPE) -j $(JOBS) 

debug:
	$(MAKE) build BUILD_TYPE=Debug ENABLE_SANITIZER=OFF

debug-asn:
	$(MAKE) build BUILD_TYPE=Debug ENABLE_SANITIZER=ON

release:
	$(MAKE) build BUILD_TYPE=Release ENABLE_SANITIZER=OFF

release-debug:
	$(MAKE) build BUILD_TYPE=RelWithDebInfo ENABLE_SANITIZER=OFF

depends:
	@mkdir -p _deps
	cd _deps && bash ../scripts/install_deps_ubuntu.sh
	rm -rf _deps

sanitizer:
	echo 1 > /proc/sys/vm/overcommit_memory

velox-patch:
	cd velox && git checkout -f && git apply --whitespace=fix ../velox.patch && git apply --whitespace=fix ../velox_cast.patch
