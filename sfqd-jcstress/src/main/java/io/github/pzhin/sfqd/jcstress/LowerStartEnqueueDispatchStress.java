package io.github.pzhin.sfqd.jcstress;

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
import org.openjdk.jcstress.infra.results.IIIII_Result;

/** Proves that dispatch orders a concurrently admitted lower-start job according to its linearization order. */
@JCStressTest
@Outcome(id = "1, 260, 1, 1, 3", expect = Expect.ACCEPTABLE,
    desc = "Lower-start admission preceded the one-job dispatch.")
@Outcome(id = "1, 258, 1, 1, 3", expect = Expect.ACCEPTABLE,
    desc = "One-job dispatch preceded lower-start admission.")
@Outcome(expect = Expect.FORBIDDEN, desc = "The concurrent priority update was lost or torn.")
@State
public class LowerStartEnqueueDispatchStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 2, 3));
  private final FlowHandle lowerStartFlow;

  /** Builds a queued high-start job while retaining the current busy period. */
  public LowerStartEnqueueDispatchStress() {
    FlowHandle highStartFlow = SchedulerTestSupport.register(scheduler, "high");
    lowerStartFlow = SchedulerTestSupport.register(scheduler, "low");
    SchedulerTestSupport.enqueue(scheduler, highStartFlow, 1, 10L);
    SchedulerTestSupport.enqueue(scheduler, highStartFlow, 2, 1L);
    JobHandle running = scheduler.dispatchUpTo(1).get(0).jobHandle();
    scheduler.complete(running);
  }

  /**
   * Admits the job whose start tag is lower than the existing head.
   *
   * @param result actor result carrier
   */
  @Actor
  public void enqueue(IIIII_Result result) {
    result.r1 = SchedulerTestSupport.enqueueCode(scheduler.enqueue(lowerStartFlow, 3, new Object(), 1L));
  }

  /**
   * Dispatches one job from the concurrently changing priority index.
   *
   * @param result actor result carrier
   */
  @Actor
  public void dispatch(IIIII_Result result) {
    result.r2 = SchedulerTestSupport.dispatchCode(scheduler.dispatchUpTo(1));
  }

  /**
   * Reports final queue, running, and admission counts.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = snapshot.queuedJobs();
    result.r4 = snapshot.runningJobs();
    result.r5 = (int) snapshot.acceptedTotal();
  }
}
