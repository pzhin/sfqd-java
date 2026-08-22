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
import org.openjdk.jcstress.infra.results.IIII_Result;

/** Proves exactly-once cancellation of one queued capability. */
@JCStressTest
@Outcome(id = "1, 3, 0, 1", expect = Expect.ACCEPTABLE, desc = "First cancellation won.")
@Outcome(id = "3, 1, 0, 1", expect = Expect.ACCEPTABLE, desc = "Second cancellation won.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Cancellation was not exactly once.")
@State
public class DuplicateCancelStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
  private final JobHandle job;

  /** Builds a stable queued-job fixture. */
  public DuplicateCancelStress() {
    FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
    job = SchedulerTestSupport.enqueue(scheduler, flow, 1, 1L);
  }

  /**
   * Attempts the first cancellation.
   *
   * @param result actor result carrier
   */
  @Actor
  public void first(IIII_Result result) {
    result.r1 = SchedulerTestSupport.cancelCode(scheduler.cancel(job));
  }

  /**
   * Attempts the competing cancellation.
   *
   * @param result actor result carrier
   */
  @Actor
  public void second(IIII_Result result) {
    result.r2 = SchedulerTestSupport.cancelCode(scheduler.cancel(job));
  }

  /**
   * Reports remaining queue size and cancellation count.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = snapshot.queuedJobs();
    result.r4 = (int) snapshot.cancelledTotal();
  }
}
