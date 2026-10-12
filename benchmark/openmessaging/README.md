# OpenMessaging Benchmark against nisshi + postgres

Runs the [OpenMessaging Benchmark](https://openmessaging.cloud/docs/benchmarks/) Kafka driver against a
single nisshi broker with the `postgres://` storage backend. Needs only docker and a Rust toolchain;
Java and Maven run inside the benchmark image.

```shell
./run.sh build                       # clone openmessaging/benchmark into .omb/ and build the image
./run.sh broker                      # postgres:18 on :5432 + nisshi (release) on :9092
./run.sh run                         # workloads/smoke.yaml, 1 minute
./run.sh run /benchmark/workloads/1-topic-16-partitions-1kb.yaml   # any upstream workload
./run.sh stop
```

Result JSON lands in `results/`. `HEAP_OPTS` overrides the benchmark JVM heap (default `-Xms512m -Xmx1g`).

- `driver-nisshi.yaml` is upstream's Kafka driver with `replicationFactor: 1` (nisshi is single node).
- The container reaches the host broker as `host.docker.internal:9092`, which the broker also advertises.
