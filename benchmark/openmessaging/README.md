# OpenMessaging Benchmark against nisshi + postgres

Runs the [OpenMessaging Benchmark](https://openmessaging.cloud/docs/benchmarks/) Kafka driver against a
single nisshi broker with the `postgres://` storage backend. Everything runs on the host: it needs a Rust
toolchain, a JDK and Maven for the benchmark, and a Homebrew `postgresql@18`. Docker is not required.

```shell
./run.sh build                       # clone openmessaging/benchmark into .omb/ and build it
./run.sh broker                      # postgres on :5432 (files in .pg/) + nisshi (release) on :9092
./run.sh run                         # workloads/smoke.yaml, 1 minute
./run.sh run workloads/16-partitions-3min.yaml                     # 16 partitions at 50k msg/s, 3 minutes
./run.sh run workloads/1-topic-16-partitions-1kb.yaml              # any upstream workload, 15 minutes
./run.sh stop
```

Result JSON lands in `results/`. `HEAP_OPTS` overrides the benchmark JVM heap (upstream default `-Xms4G -Xmx4G`).
`POSTGRES=docker` runs postgres 18 in a container instead; see the header of `run.sh` for the postgres settings.

## Checking queries against large volumes

`seed.sql` fills a topic with generated records straight in postgres, far faster than producing
through the broker, and `explain` shows the plans of the fetch queries against it. Start postgres
with `./run.sh broker` first.

```shell
./run.sh seed partitions=16 rows=1000000 value_size=1024   # ~18 GB, see seed.sql for all variables
./run.sh explain seed 3 500000                             # topic, partition, fetch offset
```

Compare plans at several row counts, and from the start, middle and tail of a partition. The seeded
topic is an ordinary topic, so a consumer pointed at the broker can read it end to end.

- `driver-nisshi.yaml` is upstream's Kafka driver with `replicationFactor: 1` (nisshi is single node).
- The broker advertises `localhost:9092`, which is also the driver's bootstrap address.
- The build checks out upstream commit `5b1fa70951a323da26bd587174b58bb2c65b0b5c` (`OMB_COMMIT` overrides it) and stops with an error if its `Benchmark.main` no longer matches the patch.
- The build overrides upstream's Lombok version, which cannot compile on a current JDK, and makes the JVM exit after a run, which upstream's `Benchmark.main` does not.
