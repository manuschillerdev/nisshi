#!/usr/bin/env bash
# Run the OpenMessaging Benchmark (https://openmessaging.cloud/docs/benchmarks/) against a
# nisshi broker backed by postgres. Java and Maven run in docker, so neither is needed on the host.
#
#   run.sh build              clone and build the benchmark image
#   run.sh broker             start postgres (docker, port 5432) and the nisshi broker
#   run.sh run [workload]     run a workload (default: workloads/smoke.yaml, relative to
#                             benchmark/openmessaging/, or a /benchmark/workloads/... path)
#   run.sh seed [var=value]   fill postgres with generated records, see seed.sql for the variables
#   run.sh explain [topic [partition [offset]]]
#                             EXPLAIN ANALYZE the fetch queries against the seeded topic
#   run.sh stop               stop the broker and postgres
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
omb="$here/.omb"
image=nisshi-openmessaging-benchmark
results="$here/results"

build() {
    [[ -d "$omb" ]] || git clone --depth 1 https://github.com/openmessaging/benchmark "$omb"
    docker build -t "$image" -f "$here/Dockerfile" "$omb"
}

broker() {
    cd "$root"
    # A dedicated postgres container, not the compose `db` service. It binds 5432, so stop any
    # other postgres or compose `db` holding that port first
    docker rm -f nisshi-omb-postgres >/dev/null 2>&1 || true
    docker run -d --name nisshi-omb-postgres -p 5432:5432 \
        -e POSTGRES_PASSWORD=postgres -e PGUSER=postgres \
        -v "$root/etc/initdb.d:/docker-entrypoint-initdb.d:ro" \
        --health-cmd=pg_isready --health-interval=2s --health-retries=30 \
        postgres:18 >/dev/null
    until [[ "$(docker inspect -f '{{.State.Health.Status}}' nisshi-omb-postgres)" == healthy ]]; do sleep 1; done
    cargo build --release --bin nisshi --no-default-features --features postgres
    pkill -f 'target/release/nisshi broker' 2>/dev/null || true
    # host.docker.internal is how the benchmark container reaches the host, and the broker
    # advertises it so clients inside the container can follow metadata back to it
    RUST_LOG=${RUST_LOG:-warn} nohup target/release/nisshi broker \
        --storage-engine=postgres://postgres:postgres@localhost \
        --advertised-listener-url=tcp://host.docker.internal:9092 \
        >"$here/broker.log" 2>&1 &
    echo $! >"$here/broker.pid"
    echo "broker pid $(cat "$here/broker.pid"), log $here/broker.log"
}

run() {
    local workload=${1:-workloads/smoke.yaml}
    [[ "$workload" == /* ]] || workload="/nisshi/$workload"
    mkdir -p "$results"
    # bin/benchmark resolves its classpath relative to /benchmark and writes result JSON to the
    # working directory, so run there and move the results out afterwards. HEAP_OPTS keeps
    # the JVM within what Docker Desktop's VM has free.
    docker run --rm \
        -e "HEAP_OPTS=${HEAP_OPTS:--Xms512m -Xmx1g}" \
        -v "$here:/nisshi:ro" -v "$results:/results" -w /benchmark \
        "$image" bash -c "bin/benchmark --drivers /nisshi/driver-nisshi.yaml $workload; mv -f *.json /results/ 2>/dev/null || true"
}

psql() {
    docker exec -i nisshi-omb-postgres psql -U postgres -X -q "$@"
}

# Generates records straight into postgres, which is far faster than producing through the broker
seed() {
    local args=() var
    for var in "$@"; do args+=(-v "$var"); done
    psql "${args[@]}" <"$here/seed.sql"
}

# Reads from the given offset up to the high watermark, as a fetch does, with a 1 MiB limit
explain() {
    local topic=${1:-seed} partition=${2:-0} offset=${3:-0} file query high key
    high=$(psql -At -c "select w.high from watermark w join topition tp on tp.id = w.topition join topic t on t.id = tp.topic where t.name = '$topic' and tp.partition = $partition")
    for file in record_fetch record_fetch_keyed; do
        echo "== $file, topic $topic partition $partition offsets $offset..$high"
        # prepared statements take their parameters from \bind, so drop the comments and final ;
        query=$(grep -v '^--' "$root/nisshi-storage-sql/src/pg/$file.sql" | sed 's/;[[:space:]]*$//')
        # only the keyed query takes the key, as $7
        if [[ $file == record_fetch_keyed ]]; then key=key-1; else key=; fi
        printf 'explain (analyze, buffers) %s\n\\bind nisshi_cluster %s %s %s 1048576 %s %s \\g\n' \
            "$query" "$topic" "$partition" "$offset" "$high" "$key" | psql
    done
}

stop() {
    [[ -f "$here/broker.pid" ]] && kill "$(cat "$here/broker.pid")" 2>/dev/null || true
    rm -f "$here/broker.pid"
    docker rm -f nisshi-omb-postgres >/dev/null 2>&1 || true
}

case "${1:-}" in
    build) build ;;
    broker) broker ;;
    run) shift; run "$@" ;;
    seed) shift; seed "$@" ;;
    explain) shift; explain "$@" ;;
    stop) stop ;;
    *) sed -n '2,14p' "$0"; exit 1 ;;
esac
