package io.github.pzhin.sfqd;

import java.util.Objects;

/**
 * Immutable limits and accounting policy for one scheduler instance.
 *
 * <p>The supported public ranges are {@code depth} in
 * {@code [1, 1_000_000]}, {@code maxFlows} in
 * {@code [1, Integer.MAX_VALUE]}, and {@code maxLiveJobs} in
 * {@code [depth, Integer.MAX_VALUE]}. These limits are fixed for the lifetime
 * of a scheduler. At every linearization point, running jobs are in
 * {@code [0, depth]}, registered flows are in {@code [0, maxFlows]}, and
 * queued plus running jobs are in {@code [0, maxLiveJobs]}.
 *
 * @param depth maximum dispatched-but-not-completed issue depth, in
 *        {@code [1, 1_000_000]}
 * @param maxFlows maximum simultaneously registered flows, in
 *        {@code [1, Integer.MAX_VALUE]}
 * @param maxLiveJobs maximum queued plus running jobs, in
 *        {@code [depth, Integer.MAX_VALUE]}
 * @param cancellationAccounting virtual fairness accounting policy for cancelled queued jobs
 */
public record SchedulerConfig(
        int depth,
        int maxFlows,
        int maxLiveJobs,
        CancellationAccounting cancellationAccounting) {
    /**
     * Maximum representable and validated issue depth.
     *
     * <p>This configuration boundary is not a claim that an atomic dispatch at this depth has practical
     * throughput, latency, allocation, or lock-hold characteristics for a particular deployment.
     */
    public static final int MAX_DEPTH = 1_000_000;

    /**
     * Creates a configuration using {@link CancellationAccounting#CHARGE_RESERVED_COST}.
     *
     * @param depth maximum outstanding issue depth, in {@code [1, 1_000_000]}
     * @param maxFlows maximum simultaneously registered flows, in {@code [1, Integer.MAX_VALUE]}
     * @param maxLiveJobs maximum queued plus running jobs, in {@code [depth, Integer.MAX_VALUE]}
     */
    public SchedulerConfig(int depth, int maxFlows, int maxLiveJobs) {
        this(depth, maxFlows, maxLiveJobs, CancellationAccounting.CHARGE_RESERVED_COST);
    }

    /**
     * Validates and creates a configuration.
     *
     * @param depth maximum outstanding issue depth, in
     *        {@code [1, 1_000_000]}
     * @param maxFlows maximum simultaneously registered flows, in
     *        {@code [1, Integer.MAX_VALUE]}
     * @param maxLiveJobs maximum queued plus running jobs, in
     *        {@code [depth, Integer.MAX_VALUE]}
     * @param cancellationAccounting virtual fairness accounting policy for cancelled queued jobs
     * @throws IllegalArgumentException if depth is outside {@code [1, 1_000_000]},
     *         maxFlows is outside {@code [1, Integer.MAX_VALUE]}, or
     *         maxLiveJobs is outside {@code [depth, Integer.MAX_VALUE]}
     * @throws NullPointerException if cancellationAccounting is null
     */
    public SchedulerConfig {
        Objects.requireNonNull(cancellationAccounting, "cancellationAccounting");
        if (depth < 1 || depth > MAX_DEPTH) {
            throw new IllegalArgumentException("depth must be in [1, 1_000_000]");
        }
        if (maxFlows < 1) {
            throw new IllegalArgumentException("maxFlows must be positive");
        }
        if (maxLiveJobs < depth) {
            throw new IllegalArgumentException("maxLiveJobs must be at least depth");
        }
    }
}
