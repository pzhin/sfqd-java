package io.github.pzhin.sfqd.jcstress;

import io.github.pzhin.sfqd.CancellationAccounting;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.JobHandle;
import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SchedulerSnapshot;
import io.github.pzhin.sfqd.SfqdScheduler;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIIIII_Result;

/** Proves refund cancellation remains atomic against dispatch of the preceding flow head. */
@JCStressTest
@Outcome(id = "257, 1, 0, 1, 1, 1", expect = Expect.ACCEPTABLE,
    desc = "One head dispatched and its queued successor refunded and cancelled.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Refund cancellation and dispatch did not form one legal history.")
@State
public class RefundUnselectedCancelDispatchStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(
          1, 1, 2, CancellationAccounting.REFUND_CANCELLED_COST));
  private final JobHandle victim;

  /** Builds an ordered head and queued refund victim. */
  public RefundUnselectedCancelDispatchStress() {
    FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
    SchedulerTestSupport.enqueue(scheduler, flow, 1, 1L);
    victim = SchedulerTestSupport.enqueue(scheduler, flow, 2, 1L);
  }

  /**
   * Dispatches only the deterministic head.
   *
   * @param result actor result carrier
   */
  @Actor
  public void dispatch(IIIIII_Result result) {
    result.r1 = SchedulerTestSupport.dispatchCode(scheduler.dispatchUpTo(1));
  }

  /**
   * Cancels and refunds the queued successor.
   *
   * @param result actor result carrier
   */
  @Actor
  public void cancel(IIIIII_Result result) {
    result.r2 = SchedulerTestSupport.cancelCode(scheduler.cancel(victim));
  }

  /**
   * Reports final state and lifecycle counters.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = snapshot.queuedJobs();
    result.r4 = snapshot.runningJobs();
    result.r5 = (int) snapshot.cancelledTotal();
    result.r6 = (int) snapshot.dispatchedTotal();
  }
}
