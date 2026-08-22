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
import org.openjdk.jcstress.infra.results.IIIIII_Result;

/** Proves the depth-one completion-versus-dispatch capacity histories. */
@JCStressTest
@Outcome(id = "1, 258, 0, 1, 1, 2", expect = Expect.ACCEPTABLE,
    desc = "Completion released the slot for exactly one successor.")
@Outcome(id = "1, 0, 1, 0, 1, 1", expect = Expect.ACCEPTABLE, desc = "Dispatch observed the occupied slot.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Capacity was lost, duplicated, or observed non-atomically.")
@State
public class CompletionDispatchStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 1, 2));
  private final JobHandle running;

  /** Builds one running job and one queued successor. */
  public CompletionDispatchStress() {
    FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
    SchedulerTestSupport.enqueue(scheduler, flow, 1, 1L);
    SchedulerTestSupport.enqueue(scheduler, flow, 2, 1L);
    running = scheduler.capacityAvailable(1).get(0).jobHandle();
  }

  /**
   * Completes the running job.
   *
   * @param result actor result carrier
   */
  @Actor
  public void complete(IIIIII_Result result) {
    result.r1 = SchedulerTestSupport.completionCode(scheduler.complete(running));
  }

  /**
   * Attempts to dispatch the queued successor.
   *
   * @param result actor result carrier
   */
  @Actor
  public void dispatch(IIIIII_Result result) {
    result.r2 = SchedulerTestSupport.dispatchCode(scheduler.capacityAvailable(1));
  }

  /**
   * Reports final capacity and lifecycle counters.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = snapshot.queuedJobs();
    result.r4 = snapshot.runningJobs();
    result.r5 = (int) snapshot.completedTotal();
    result.r6 = (int) snapshot.dispatchedTotal();
  }
}
