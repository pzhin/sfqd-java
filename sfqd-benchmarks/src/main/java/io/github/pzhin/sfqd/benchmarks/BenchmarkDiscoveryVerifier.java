package io.github.pzhin.sfqd.benchmarks;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

/** Verifies that the executable benchmark JAR exposes the complete expected workload set. */
public final class BenchmarkDiscoveryVerifier {
    private static final Set<String> EXPECTED = Set.of(
            "io.github.pzhin.sfqd.benchmarks.CancellationCycleBenchmark.cancelAndEnqueue",
            "io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p1c1Latency",
            "io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p1c1Throughput",
            "io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p1c3Latency",
            "io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p1c3Throughput",
            "io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p3c1Latency",
            "io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p3c1Throughput",
            "io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p4c4Latency",
            "io.github.pzhin.sfqd.benchmarks.ContentionBenchmark.p4c4Throughput",
            "io.github.pzhin.sfqd.benchmarks.FirstBusyPeriodCycleBenchmark.enqueueCancelCycle",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.cancelLastQueuedAllTagged",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.cancelLastQueuedOneTagged",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.cancelQueued",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.completeLastRunning",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.completeLastRunningAllTagged",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.completeSteady",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.dispatchBatch",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.dispatchOne",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.enqueueBackloggedTail",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.enqueueFirstBusyPeriod",
            "io.github.pzhin.sfqd.benchmarks.OperationLatencyBenchmark.enqueueInactiveFlow",
            "io.github.pzhin.sfqd.benchmarks.PerformanceScaleBenchmark.continuousBusyPeriodCycle",
            "io.github.pzhin.sfqd.benchmarks.PerformanceScaleBenchmark.dispatchBatch",
            "io.github.pzhin.sfqd.benchmarks.SteadyStateCycleBenchmark.batchCycle",
            "io.github.pzhin.sfqd.benchmarks.SteadyStateCycleBenchmark.singleJobCycle");

    private BenchmarkDiscoveryVerifier() {
    }

    /**
     * Starts the shaded JAR with {@code -l} and checks the discovered benchmark names.
     *
     * @param arguments the executable benchmark JAR path
     * @throws IOException when the process cannot be started or read
     * @throws InterruptedException when interrupted while waiting for the process
     */
    public static void main(String[] arguments) throws IOException, InterruptedException {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("expected BENCHMARK_JAR");
        }
        Path benchmarkJar = Path.of(arguments[0]);
        if (!Files.isRegularFile(benchmarkJar)) {
            throw new IllegalStateException("benchmark JAR is missing: " + benchmarkJar);
        }

        Process process = new ProcessBuilder(javaExecutable().toString(), "-jar", benchmarkJar.toString(), "-l")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("benchmark discovery exited with " + exitCode + ":\n" + output);
        }

        Set<String> actual = parseBenchmarks(output);
        if (!EXPECTED.equals(actual)) {
            throw new IllegalStateException(
                    "executable benchmark discovery differs from the expected set; expected="
                            + new TreeSet<>(EXPECTED) + "; actual=" + actual);
        }
        System.out.println("BENCHMARK_DISCOVERY PASS count=" + actual.size());
    }

    private static Set<String> parseBenchmarks(String output) {
        Set<String> actual = new TreeSet<>();
        boolean benchmarksFollow = false;
        for (String line : output.lines().toList()) {
            if (benchmarksFollow && !line.isBlank()) {
                actual.add(line.strip());
            }
            if (line.strip().equals("Benchmarks:")) {
                benchmarksFollow = true;
            }
        }
        return actual;
    }

    private static Path javaExecutable() {
        String executableName = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        return Path.of(System.getProperty("java.home"), "bin", executableName);
    }
}
