package io.github.pzhin.sfqd.benchmarks;

import io.github.pzhin.sfqd.benchmarks.PerformanceScaleBenchmark.BatchLimit;
import io.github.pzhin.sfqd.benchmarks.PerformanceScaleBenchmark.Population;
import io.github.pzhin.sfqd.benchmarks.PerformanceScaleBenchmark.ScaleFixture;
import io.github.pzhin.sfqd.benchmarks.SchedulerBenchmarkSupport.WeightModel;

/** Executable validation of the performance-scale matrix and its largest fixture. */
public final class PerformanceScaleWorkloadSmoke {
    private static final int[] BACKLOGGED_FLOWS = {1, 10, 100, 1_000, 10_000};
    private static final int[] QUEUED_JOBS = {1_000, 10_000, 100_000};

    private PerformanceScaleWorkloadSmoke() {
    }

    /**
     * Validates matrix completeness and exercises representative endpoints without producing benchmark scores.
     *
     * @param arguments no arguments are accepted
     */
    public static void main(String[] arguments) {
        if (arguments.length != 0) {
            throw new IllegalArgumentException("performance-scale workload smoke accepts no arguments");
        }
        verifyPopulationMatrix();
        verifyCoprimeWeights();
        exercise(Population.B00001_Q001000, 1, WeightModel.EQUAL, BatchLimit.ONE);
        exercise(Population.B00100_Q010000, 64, WeightModel.PAIRWISE_COPRIME, BatchLimit.SIXTEEN);
        exercise(Population.B10000_Q100000, 1_024, WeightModel.PAIRWISE_COPRIME, BatchLimit.FULL_DEPTH);
        System.out.println("PERFORMANCE_SCALE_WORKLOAD_SMOKE PASS populations="
                + Population.values().length + " fixtureCases=3");
    }

    private static void verifyPopulationMatrix() {
        int expected = 0;
        for (int queuedJobs : QUEUED_JOBS) {
            for (int backloggedFlows : BACKLOGGED_FLOWS) {
                if (backloggedFlows <= queuedJobs) {
                    expected++;
                    requirePopulation(backloggedFlows, queuedJobs);
                }
            }
        }
        if (Population.values().length != expected) {
            throw new IllegalStateException("performance population matrix is incomplete");
        }
    }

    private static void requirePopulation(int backloggedFlows, int queuedJobs) {
        for (Population population : Population.values()) {
            if (population.backloggedFlows() == backloggedFlows && population.queuedJobs() == queuedJobs) {
                return;
            }
        }
        throw new IllegalStateException("missing compatible B/Q population point");
    }

    private static void verifyCoprimeWeights() {
        long previous = 1L;
        for (int index = 0; index <= SchedulerBenchmarkSupport.MAX_PAIRWISE_COPRIME_FLOWS; index++) {
            long candidate = SchedulerBenchmarkSupport.weight(index, WeightModel.PAIRWISE_COPRIME);
            if (candidate <= previous || !isPrime(candidate)) {
                throw new IllegalStateException("pairwise-coprime benchmark weights are not distinct primes");
            }
            previous = candidate;
        }
    }

    private static boolean isPrime(long value) {
        for (long divisor = 2L; divisor * divisor <= value; divisor++) {
            if (value % divisor == 0L) {
                return false;
            }
        }
        return true;
    }

    private static void exercise(
            Population population, int depth, WeightModel weightModel, BatchLimit batchLimit) {
        ScaleFixture fixture = new ScaleFixture(population, depth, weightModel);
        int batchSize = batchLimit.effectiveSize(depth, population.queuedJobs());
        if (fixture.cycle(batchSize) != batchSize) {
            throw new IllegalStateException("performance-scale cycle cardinality diverged");
        }
        fixture.verifySteadyState();
    }
}
