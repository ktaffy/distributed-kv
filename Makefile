remove:
	rm -r build data install logs && rm client_example distributed_kv
build:
	./scripts/build.sh
run:
	./scripts/run_cluster.sh
.PHONY: remove build run