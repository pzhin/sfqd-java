package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

class SfqdFairnessBoundProperties {
    @Property(tries = 120)
    void jinTheoremTwoBoundHoldsAtEveryGeneratedCompletionPrefix(
            @ForAll("theoremScenarios") Scenario scenario) {
        runTheoremTrace(
                scenario.depth,
                scenario.weights,
                scenario.horizon,
                scenario.selectors,
                (flow, job) -> generatedCost(scenario.costSeed, flow, job),
                scenario.toString());
    }

    @Test
    void theoremBoundHoldsForEveryTraceInExplicitSmallDomain() {
        int traces = 0;
        int horizon = 4;
        for (int depth = 1; depth <= 2; depth++) {
            int selectorSequences = integerPower(depth, horizon);
            for (int firstWeight = 1; firstWeight <= 2; firstWeight++) {
                for (int secondWeight = 1; secondWeight <= 2; secondWeight++) {
                    for (int firstCost = 1; firstCost <= 3; firstCost++) {
                        for (int secondCost = 1; secondCost <= 3; secondCost++) {
                            for (int selectorCode = 0; selectorCode < selectorSequences; selectorCode++) {
                                List<Integer> selectors = selectorDigits(selectorCode, depth, horizon);
                                List<Integer> costs = List.of(firstCost, secondCost);
                                String description = "small-domain depth=" + depth
                                        + ", weights=" + List.of(firstWeight, secondWeight)
                                        + ", costs=" + costs + ", selectors=" + selectors;
                                runTheoremTrace(
                                        depth,
                                        List.of(firstWeight, secondWeight),
                                        horizon,
                                        selectors,
                                        (flow, job) -> costs.get(flow),
                                        description);
                                traces++;
                            }
                        }
                    }
                }
            }
        }

        assertEquals(612, traces);
    }

    @Property(tries = 100)
    void largerMaximumCostDefeatsSampledCostIndependentCandidate(
            @ForAll @LongRange(min = 1L, max = 1_000_000L) long candidateBound) {
        long maximumCost = candidateBound + 1L;
        TraceResult result = runTheoremTrace(
                1,
                List.of(1, 1),
                1,
                List.of(0),
                (flow, job) -> flow == 0 && job == 0 ? maximumCost : 1L,
                "maximum-cost witness M=" + maximumCost);
        BigInteger discrepancy = result.completedService.get(0).subtract(result.completedService.get(1)).abs();

        assertEquals(BigInteger.valueOf(maximumCost), discrepancy);
        assertTrue(discrepancy.compareTo(BigInteger.valueOf(candidateBound)) > 0);
        assertEquals(BigInteger.valueOf(maximumCost), result.maximumCosts.get(0));
    }

    @Provide
    Arbitrary<Scenario> theoremScenarios() {
        return Arbitraries.integers().between(2, 5).flatMap(flowCount -> Combinators.combine(
                        Arbitraries.integers().between(1, 16),
                        Arbitraries.integers().between(1, 32).list().ofSize(flowCount),
                        Arbitraries.integers().between(12, 48),
                        Arbitraries.integers().between(-1_000_000, 1_000_000))
                .as(ScenarioSeed::new)
                .flatMap(seed -> Arbitraries.integers().between(-1_000_000, 1_000_000)
                        .list().ofSize(seed.horizon)
                        .map(selectors -> new Scenario(
                                seed.depth, seed.weights, seed.horizon, seed.costSeed, selectors))));
    }

    private static TraceResult runTheoremTrace(
            int depth,
            List<Integer> weights,
            int horizon,
            List<Integer> selectors,
            CostOracle costOracle,
            String description) {
        assertEquals(horizon, selectors.size());
        int jobsPerFlow = depth + horizon + 1;
        int maxLiveJobs = Math.multiplyExact(weights.size(), jobsPerFlow);
        SfqdScheduler<String, String, Payload> scheduler =
                new SfqdScheduler<>(new SchedulerConfig(depth, weights.size(), maxLiveJobs));
        List<FlowHandle> flows = new ArrayList<>(weights.size());
        for (int flow = 0; flow < weights.size(); flow++) {
            flows.add(registered(scheduler.registerFlow("flow-" + flow, weights.get(flow))));
        }
        Map<JobHandle, ExpectedJob> expectedJobs = new HashMap<>();
        Set<JobHandle> acceptedHandles = new HashSet<>();
        List<BigInteger> maximumCosts = zeroVector(weights.size());
        for (int flow = 0; flow < weights.size(); flow++) {
            for (int job = 0; job < jobsPerFlow; job++) {
                long cost = costOracle.cost(flow, job);
                assertTrue(cost > 0L, "test cost oracle must be positive");
                String flowId = "flow-" + flow;
                String jobId = flowId + "-job-" + job;
                Payload payload = new Payload();
                JobHandle handle = accepted(scheduler.enqueue(flows.get(flow), jobId, payload, cost));
                assertTrue(acceptedHandles.add(handle));
                expectedJobs.put(handle, new ExpectedJob(flow, flowId, jobId, payload, cost));
                maximumCosts.set(flow, maximumCosts.get(flow).max(BigInteger.valueOf(cost)));
            }
        }

        Set<JobHandle> dispatchedHandles = new HashSet<>();
        List<Dispatch<String, String, Payload>> running = new ArrayList<>(scheduler.capacityAvailable(depth));
        assertEquals(depth, running.size());
        for (Dispatch<String, String, Payload> dispatch : running) {
            assertDispatch(dispatch, acceptedHandles, dispatchedHandles, expectedJobs);
        }
        assertEquals(weights.size(), scheduler.snapshot().backloggedFlows());

        List<BigInteger> completedService = zeroVector(weights.size());
        List<String> completionTrace = new ArrayList<>(horizon);
        for (int event = 0; event < horizon; event++) {
            int prefix = event + 1;
            int selectedIndex = Math.floorMod(selectors.get(event), running.size());
            Dispatch<String, String, Payload> completed = running.remove(selectedIndex);
            ExpectedJob completedExpected = expectedJobs.get(completed.jobHandle());
            assertNotNull(completedExpected);
            assertEquals(CompletionResult.COMPLETED, scheduler.complete(completed.jobHandle()));
            int flow = completedExpected.flowIndex;
            completedService.set(
                    flow, completedService.get(flow).add(BigInteger.valueOf(completedExpected.cost)));
            completionTrace.add(completedExpected.jobId);

            assertEquals(
                    weights.size(),
                    scheduler.snapshot().backloggedFlows(),
                    () -> failureContext(description, prefix, -1, -1, completedService,
                            BigInteger.ZERO, BigInteger.ZERO, completionTrace));
            assertAllPairBounds(
                    depth,
                    weights,
                    completedService,
                    maximumCosts,
                    prefix,
                    completionTrace,
                    description);

            List<Dispatch<String, String, Payload>> refill = scheduler.capacityAvailable(1);
            assertEquals(
                    1,
                    refill.size(),
                    () -> "expected exact one-slot refill; " + failureContext(
                            description,
                            prefix,
                            -1,
                            -1,
                            completedService,
                            BigInteger.ZERO,
                            BigInteger.ZERO,
                            completionTrace));
            Dispatch<String, String, Payload> replacement = refill.getFirst();
            assertDispatch(replacement, acceptedHandles, dispatchedHandles, expectedJobs);
            running.add(replacement);
            assertEquals(depth, running.size());
            assertEquals(weights.size(), scheduler.snapshot().backloggedFlows());
        }
        return new TraceResult(List.copyOf(completedService), List.copyOf(maximumCosts));
    }

    private static void assertAllPairBounds(
            int depth,
            List<Integer> weights,
            List<BigInteger> completedService,
            List<BigInteger> maximumCosts,
            int prefix,
            List<String> completionTrace,
            String description) {
        BigInteger depthFactor = BigInteger.valueOf((long) depth + 1L);
        for (int first = 0; first < weights.size(); first++) {
            for (int second = first + 1; second < weights.size(); second++) {
                BigInteger firstWeight = BigInteger.valueOf(weights.get(first));
                BigInteger secondWeight = BigInteger.valueOf(weights.get(second));
                BigInteger firstService = completedService.get(first);
                BigInteger secondService = completedService.get(second);
                BigInteger left = firstService.multiply(secondWeight)
                        .subtract(secondService.multiply(firstWeight))
                        .abs();
                BigInteger right = depthFactor.multiply(
                        maximumCosts.get(first).multiply(secondWeight)
                                .add(maximumCosts.get(second).multiply(firstWeight)));
                String context = failureContext(
                        description, prefix, first, second, completedService, left, right, completionTrace);
                assertTrue(left.compareTo(right) <= 0, context);

                BigInteger pairService = firstService.add(secondService);
                BigInteger shareNumerator = firstService.multiply(firstWeight.add(secondWeight))
                        .subtract(pairService.multiply(firstWeight))
                        .abs();
                assertEquals(left, shareNumerator, "normalized-share algebra; " + context);
                if (pairService.signum() > 0) {
                    BigInteger shareDenominator = pairService.multiply(firstWeight.add(secondWeight));
                    assertTrue(shareDenominator.signum() > 0, "positive normalized-share denominator; " + context);
                    assertTrue(shareNumerator.compareTo(right) <= 0, "normalized-share bound; " + context);
                }
            }
        }
    }

    private static void assertDispatch(
            Dispatch<String, String, Payload> dispatch,
            Set<JobHandle> acceptedHandles,
            Set<JobHandle> dispatchedHandles,
            Map<JobHandle, ExpectedJob> expectedJobs) {
        assertTrue(acceptedHandles.contains(dispatch.jobHandle()));
        assertTrue(dispatchedHandles.add(dispatch.jobHandle()));
        ExpectedJob expected = expectedJobs.get(dispatch.jobHandle());
        assertNotNull(expected);
        assertEquals(expected.flowId, dispatch.flowId());
        assertEquals(expected.jobId, dispatch.jobId());
        assertSame(expected.payload, dispatch.payload());
        assertEquals(expected.cost, dispatch.cost());
    }

    private static String failureContext(
            String description,
            int prefix,
            int first,
            int second,
            List<BigInteger> completedService,
            BigInteger left,
            BigInteger right,
            List<String> completionTrace) {
        return description + "; prefix=" + prefix + "; pair=" + first + ',' + second
                + "; W=" + completedService + "; lhs=" + left + "; rhs=" + right
                + "; completionTrace=" + completionTrace;
    }

    private static List<BigInteger> zeroVector(int size) {
        List<BigInteger> values = new ArrayList<>(size);
        for (int index = 0; index < size; index++) {
            values.add(BigInteger.ZERO);
        }
        return values;
    }

    private static long generatedCost(int seed, int flow, int job) {
        long mixed = (long) seed * 0x9E3779B9L
                + (long) flow * 0xC2B2AE35L
                + (long) job * 0x165667B1L;
        return 1L + Math.floorMod(mixed, 64L);
    }

    private static List<Integer> selectorDigits(int code, int radix, int length) {
        List<Integer> selectors = new ArrayList<>(length);
        int remaining = code;
        for (int index = 0; index < length; index++) {
            selectors.add(remaining % radix);
            remaining /= radix;
        }
        return selectors;
    }

    private static int integerPower(int base, int exponent) {
        int result = 1;
        for (int count = 0; count < exponent; count++) {
            result = Math.multiplyExact(result, base);
        }
        return result;
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return ((RegisterFlowResult.Registered) result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return ((EnqueueResult.Accepted) result).jobHandle();
    }

    private interface CostOracle {
        long cost(int flow, int job);
    }

    private record ScenarioSeed(int depth, List<Integer> weights, int horizon, int costSeed) {
    }

    private record Scenario(
            int depth, List<Integer> weights, int horizon, int costSeed, List<Integer> selectors) {
    }

    private record ExpectedJob(
            int flowIndex, String flowId, String jobId, Payload payload, long cost) {
    }

    private record TraceResult(List<BigInteger> completedService, List<BigInteger> maximumCosts) {
    }

    private static final class Payload {
    }
}
