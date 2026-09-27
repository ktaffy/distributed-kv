remove:
	rm -r build data install logs && rm client_example distributed_kv
build:
	./scripts/build.sh
run:
	./scripts/run_cluster.sh

test:
	cmake -S . -B build
	cmake --build build --target unit_tests -j
	ctest --test-dir build --output-on-failure

.PHONY: remove build run test