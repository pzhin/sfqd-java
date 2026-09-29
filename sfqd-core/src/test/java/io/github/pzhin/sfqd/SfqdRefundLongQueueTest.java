package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

final class SfqdRefundLongQueueTest {
    @Test
    void arbitraryLongQueueCancellationsAndReplacementsMatchEagerOracle() {
        SchedulerConfig config = new SchedulerConfig(16, 3, 4000,
                CancellationAccounting.REFUND_CANCELLED_COST);
        SfqdScheduler<Integer, Integer, String> actual = new SfqdScheduler<>(config);
        ReferenceScheduler<Integer, Integer, String> expected = new ReferenceScheduler<>(config);
        FlowHandle[] flows = new FlowHandle[3];
        FlowHandle[] oracleFlows = new FlowHandle[3];
        long[] weights = {6, 35, 16};
        for (int index = 0; index < flows.length; index++) {
            flows[index] = registered(actual.registerFlow(index, weights[index]));
            oracleFlows[index] = registered(expected.registerFlow(index, weights[index]));
        }
        List<JobHandle> handles = new ArrayList<>();
        List<JobHandle> oracleHandles = new ArrayList<>();
        SplittableRandom random = new SplittableRandom(793117L);
        for (int id = 0; id < 3000; id++) {
            int flow = id < 2500 ? 0 : id % 3;
            long cost = id % 11 == 0 ? Long.MAX_VALUE : 1 + random.nextInt(1000);
            handles.add(accepted(actual.enqueue(flows[flow], id, "payload", cost)));
            oracleHandles.add(accepted(expected.enqueue(oracleFlows[flow], id, "payload", cost)));
        }
        for (int step = 0; step < 3000; step++) {
            int index = step % 7 == 0 ? step / 7 : random.nextInt(handles.size());
            assertEquals(expected.cancel(oracleHandles.get(index)), actual.cancel(handles.get(index)));
            if (step % 5 == 0) {
                List<Dispatch<Integer, Integer, String>> wanted = expected.dispatchUpTo(16);
                List<Dispatch<Integer, Integer, String>> got = actual.dispatchUpTo(16);
                assertEquals(wanted.stream().map(Dispatch::jobId).toList(),
                        got.stream().map(Dispatch::jobId).toList());
                for (int job = 0; job < got.size(); job++) {
                    assertEquals(expected.complete(wanted.get(job).jobHandle()),
                            actual.complete(got.get(job).jobHandle()));
                }
            }
            int flow = step % 3;
            int id = 3000 + step;
            handles.add(accepted(actual.enqueue(flows[flow], id, "replacement", 1 + step % 37)));
            oracleHandles.add(accepted(expected.enqueue(oracleFlows[flow], id, "replacement", 1 + step % 37)));
            assertEquals(expected.snapshot(), actual.snapshot());
            assertEquals(expected.snapshot(oracleFlows[flow]), actual.snapshot(flows[flow]));
        }
        while (actual.snapshot().queuedJobs() > 0) {
            List<Dispatch<Integer, Integer, String>> wanted = expected.dispatchUpTo(16);
            List<Dispatch<Integer, Integer, String>> got = actual.dispatchUpTo(16);
            assertEquals(wanted.stream().map(Dispatch::jobId).toList(),
                    got.stream().map(Dispatch::jobId).toList());
            for (int job = 0; job < got.size(); job++) {
                assertEquals(expected.complete(wanted.get(job).jobHandle()),
                        actual.complete(got.get(job).jobHandle()));
            }
        }
        assertEquals(expected.snapshot(), actual.snapshot());
    }

    private static FlowHandle registered(RegisterFlowResult result) {
        return assertInstanceOf(RegisterFlowResult.Registered.class, result).flowHandle();
    }

    private static JobHandle accepted(EnqueueResult result) {
        return assertInstanceOf(EnqueueResult.Accepted.class, result).jobHandle();
    }
}
