BUILD_TYPE ?= Release

build:
	cmake -S . -B build -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)
	cmake --build build -j
	ln -sf build/distributed_kv distributed_kv
	ln -sf build/client_example client_example

test:
	cmake -S . -B build
	cmake --build build --target unit_tests -j
	ctest --test-dir build --output-on-failure

run: build
	./scripts/run_cluster.sh

clean:
	rm -rf build data logs install distributed_kv client_example

.PHONY: build test run clean