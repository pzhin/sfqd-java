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

/** Proves the documented winner semantics when dispatch selects the job being cancelled. */
@JCStressTest
@Outcome(id = "257, 2, 0, 1, 0, 1", expect = Expect.ACCEPTABLE, desc = "Dispatch won with one job.")
@Outcome(id = "0, 1, 0, 0, 1, 0", expect = Expect.ACCEPTABLE, desc = "Cancellation won.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Dispatch and cancellation did not form one legal history.")
@State
public class SelectedCancelDispatchStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
  private final JobHandle job;

  /** Builds a stable single-job fixture. */
  public SelectedCancelDispatchStress() {
    FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
    job = SchedulerTestSupport.enqueue(scheduler, flow, 1, 1L);
  }

  /**
   * Attempts to select the job.
   *
   * @param result actor result carrier
   */
  @Actor
  public void dispatch(IIIIII_Result result) {
    result.r1 = SchedulerTestSupport.dispatchCode(scheduler.capacityAvailable(1));
  }

  /**
   * Attempts to cancel the same job.
   *
   * @param result actor result carrier
   */
  @Actor
  public void cancel(IIIIII_Result result) {
    result.r2 = SchedulerTestSupport.cancelCode(scheduler.cancel(job));
  }

  /**
   * Reports final queue, capacity, and cumulative counters.
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
