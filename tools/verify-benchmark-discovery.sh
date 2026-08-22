#!/usr/bin/env bash
set -euo pipefail

JAVA_BIN=${1:?java executable is required}
BENCHMARK_JAR=${2:?benchmark JAR is required}

actual=$(
  "$JAVA_BIN" -jar "$BENCHMARK_JAR" -l |
    awk 'seen && NF { print } /^Benchmarks:[[:space:]]*$/ { seen = 1 }' |
    sort
)

expected=$(sort <<'EOF'
io.github.pzhin.sfqd.benchmarks.CancellationCycleBenchmark.cancelAndEnqueue
io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p1c1Latency
io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p1c1Throughput
io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p1c3Latency
io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p1c3Throughput
io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p3c1Latency
io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p3c1Throughput
io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p4c4Latency
io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p4c4Throughput
io.github.pzhin.sfqd.benchmarks.FirstBusyPeriodCycleBenchmark.enqueueCancelCycle
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.cancelLastQueuedAllTagged
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.cancelLastQueuedOneTagged
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.cancelQueued
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.completeLastRunning
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.completeLastRunningAllTagged
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.completeSteady
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.dispatchBatch
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.dispatchOne
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.enqueueBackloggedTail
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.enqueueFirstBusyPeriod
io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.enqueueInactiveFlow
io.github.pzhin.sfqd.benchmarks.SteadyStateCycleBenchmark.batchCycle
io.github.pzhin.sfqd.benchmarks.SteadyStateCycleBenchmark.singleJobCycle
EOF
)

if [[ "$actual" != "$expected" ]]; then
  echo "ERROR: executable benchmark discovery differs from the expected set" >&2
  diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2 || true
  exit 1
fi

count=$(printf '%s\n' "$actual" | wc -l | tr -d ' ')
if [[ "$count" != 23 ]]; then
  echo "ERROR: expected 23 benchmarks, found $count" >&2
  exit 1
fi

echo "BENCHMARK_DISCOVERY PASS count=$count"
