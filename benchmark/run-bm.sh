#!/bin/bash
set -eu

root_dir=$(dirname $0)/..
name="$1"
runner="$2"
out_dir="$3"

$root_dir/gradlew --no-daemon clean benchmark:shadowJar

function run_with_opts() {
    id=$1; shift
    tmp=$(mktemp)
    $root_dir/benchmark/debm.sh \
        --runs 1 \
        --title "$name-$id" \
        --format=json \
        --runner "$runner" \
        --profile \
        --profiler-opts="-f $out_dir/$name-profile.html" \
        --file-name-only \
        --warmup 10000000 \
        --param=decaton.max.pending.records=10000 \
        "$@"
    mv $tmp $out_dir/$name-benchmark.json
}

#run_with_opts "tasks_100k_latency_10ms_concurrency_20" --tasks 100000 --simulate-latency=10 --param=decaton.partition.concurrency=20
run_with_opts "tasks_1000k_latency_0ms_concurrency_20" --tasks 1000000 --simulate-latency=0 --param=decaton.partition.concurrency=20
