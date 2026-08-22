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

/** Proves that duplicate completion releases one issue slot exactly once. */
@JCStressTest
@Outcome(id = "1, 3, 0, 1, 1", expect = Expect.ACCEPTABLE, desc = "First completion won.")
@Outcome(id = "3, 1, 0, 1, 1", expect = Expect.ACCEPTABLE, desc = "Second completion won.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Completion or slot release was not exactly once.")
@State
public class DuplicateCompletionStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
  private final JobHandle job;

  /** Builds a stable running-job fixture. */
  public DuplicateCompletionStress() {
    FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
    SchedulerTestSupport.enqueue(scheduler, flow, 1, 1L);
    job = scheduler.dispatchUpTo(1).get(0).jobHandle();
  }

  /**
   * Attempts the first completion.
   *
   * @param result actor result carrier
   */
  @Actor
  public void first(IIIII_Result result) {
    result.r1 = SchedulerTestSupport.completionCode(scheduler.complete(job));
  }

  /**
   * Attempts the competing completion.
   *
   * @param result actor result carrier
   */
  @Actor
  public void second(IIIII_Result result) {
    result.r2 = SchedulerTestSupport.completionCode(scheduler.complete(job));
  }

  /**
   * Reports final running count, free slots, and completion count.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = snapshot.runningJobs();
    result.r4 = snapshot.freeSlots();
    result.r5 = (int) snapshot.completedTotal();
  }
}
