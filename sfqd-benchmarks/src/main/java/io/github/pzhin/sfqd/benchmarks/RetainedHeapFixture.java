package io.github.pzhin.sfqd.benchmarks;

import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.RegisterFlowResult;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.SfqdScheduler;
import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.List;

/** Isolated process fixture for external retained-heap histograms; it does not calculate memory/job. */
public final class RetainedHeapFixture {
    private RetainedHeapFixture() {
    }

    /**
     * Builds the requested live scheduler state, prints an exact readiness record, and keeps it strongly reachable.
     *
     * @param arguments named options documented in the benchmark README
     * @throws InterruptedException if the holding process is interrupted
     */
    public static void main(String[] arguments) throws InterruptedException {
        FixtureArguments options = FixtureArguments.parse(arguments);
        SfqdScheduler<FixtureFlow, FixtureJob, FixturePayload> scheduler = build(options);
        SchedulerSnapshot snapshot = scheduler.snapshot();
        requireSnapshot(options, snapshot);

        System.out.println("PID=" + ProcessHandle.current().pid());
        System.out.println("CONFIG flowCount=" + options.flowCount()
                + " queuedJobs=" + options.queuedJobs()
                + " runningJobs=" + options.runningJobs()
                + " depth=" + options.depth()
                + " holdSeconds=" + options.holdSeconds());
        System.out.println("SNAPSHOT=" + snapshot);
        System.out.println("READY");
        System.out.flush();

        holdReachable(scheduler, options.holdSeconds());
    }

    private static SfqdScheduler<FixtureFlow, FixtureJob, FixturePayload> build(FixtureArguments options) {
        int totalJobs = Math.addExact(options.queuedJobs(), options.runningJobs());
        int maxLiveJobs = Math.max(options.depth(), totalJobs);
        SfqdScheduler<FixtureFlow, FixtureJob, FixturePayload> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(options.depth(), options.flowCount(), maxLiveJobs));
        List<FlowHandle> flows = new ArrayList<>(options.flowCount());
        for (int index = 0; index < options.flowCount(); index++) {
            RegisterFlowResult registration = scheduler.registerFlow(new FixtureFlow(index), 1L);
            if (!(registration instanceof RegisterFlowResult.Registered registered)) {
                throw new IllegalStateException("fixture registration rejected: " + registration);
            }
            flows.add(registered.flowHandle());
        }
        for (int index = 0; index < totalJobs; index++) {
            FixtureJob job = new FixtureJob(index);
            EnqueueResult result = scheduler.enqueue(
                    flows.get(index % flows.size()), job, FixturePayload.INSTANCE, 1L);
            if (!(result instanceof EnqueueResult.Accepted)) {
                throw new IllegalStateException("fixture enqueue rejected at index " + index + ": " + result);
            }
        }
        List<Dispatch<FixtureFlow, FixtureJob, FixturePayload>> running =
                scheduler.dispatchUpTo(options.runningJobs());
        if (running.size() != options.runningJobs()) {
            throw new IllegalStateException("fixture failed to establish requested running jobs");
        }
        return scheduler;
    }

    private static void requireSnapshot(FixtureArguments options, SchedulerSnapshot snapshot) {
        int expectedActive = options.queuedJobs() + options.runningJobs() == 0
                ? 0 : Math.min(options.flowCount(), options.queuedJobs() + options.runningJobs());
        if (snapshot.registeredFlows() != options.flowCount()
                || snapshot.queuedJobs() != options.queuedJobs()
                || snapshot.runningJobs() != options.runningJobs()
                || snapshot.activeFlows() != expectedActive) {
            throw new IllegalStateException("fixture snapshot mismatch: " + snapshot);
        }
    }

    private static void holdReachable(Object scheduler, int holdSeconds) throws InterruptedException {
        long deadline = holdSeconds == 0
                ? Long.MAX_VALUE : System.nanoTime() + holdSeconds * 1_000_000_000L;
        while (System.nanoTime() < deadline) {
            Thread.sleep(250L);
            Reference.reachabilityFence(scheduler);
        }
    }

    /**
     * Parsed isolated-process fixture options.
     *
     * @param flowCount registered flow count
     * @param queuedJobs requested queued-job count at readiness
     * @param runningJobs requested running-job count at readiness
     * @param depth scheduler issue depth
     * @param holdSeconds seconds to retain state, or zero to wait indefinitely
     */
    private record FixtureArguments(int flowCount, int queuedJobs, int runningJobs, int depth, int holdSeconds) {
        private static FixtureArguments parse(String[] arguments) {
            int flowCount = -1;
            int queuedJobs = -1;
            int runningJobs = -1;
            int depth = -1;
            int holdSeconds = 0;
            for (String argument : arguments) {
                int separator = argument.indexOf('=');
                if (!argument.startsWith("--") || separator < 3 || separator == argument.length() - 1) {
                    throw usage("invalid option: " + argument);
                }
                String name = argument.substring(2, separator);
                int value;
                try {
                    value = Integer.parseInt(argument.substring(separator + 1));
                } catch (NumberFormatException invalidNumber) {
                    throw usage("invalid integer for " + name);
                }
                switch (name) {
                    case "flowCount" -> flowCount = value;
                    case "queuedJobs" -> queuedJobs = value;
                    case "runningJobs" -> runningJobs = value;
                    case "depth" -> depth = value;
                    case "holdSeconds" -> holdSeconds = value;
                    default -> throw usage("unknown option: " + name);
                }
            }
            if (flowCount < 1 || queuedJobs < 0 || runningJobs < 0 || depth < 1 || holdSeconds < 0) {
                throw usage("missing or out-of-range option");
            }
            if (runningJobs > depth) {
                throw usage("runningJobs must not exceed depth");
            }
            long totalJobs = (long) queuedJobs + runningJobs;
            if (totalJobs > Integer.MAX_VALUE) {
                throw usage("queuedJobs + runningJobs exceeds Integer.MAX_VALUE");
            }
            return new FixtureArguments(flowCount, queuedJobs, runningJobs, depth, holdSeconds);
        }

        private static IllegalArgumentException usage(String detail) {
            return new IllegalArgumentException(detail + "; required: --flowCount=N --queuedJobs=N "
                    + "--runningJobs=N --depth=N [--holdSeconds=N]");
        }
    }

    /**
     * Stable isolated-process flow identifier.
     *
     * @param index fixture flow index
     */
    private record FixtureFlow(int index) {
    }

    /**
     * Stable isolated-process job identifier.
     *
     * @param index fixture job index
     */
    private record FixtureJob(int index) {
    }

    /** Shared payload isolates scheduler metadata from caller payload size. */
    private enum FixturePayload {
        INSTANCE
    }
}
