package io.github.pzhin.sfqd.jcstress;

import io.github.pzhin.sfqd.FlowHandle;
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

/** Proves that two depth-two dispatch calls cannot split one atomic batch. */
@JCStressTest
@Outcome(id = "515, 0, 0, 2, 2", expect = Expect.ACCEPTABLE, desc = "First actor obtained both jobs once.")
@Outcome(id = "0, 515, 0, 2, 2", expect = Expect.ACCEPTABLE, desc = "Second actor obtained both jobs once.")
@Outcome(expect = Expect.FORBIDDEN, desc = "The atomic depth-two batch was split or corrupted.")
@State
public class AtomicBatchDispatchStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(2, 1, 2));

  /** Builds a stable two-job fixture. */
  public AtomicBatchDispatchStress() {
    FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
    SchedulerTestSupport.enqueue(scheduler, flow, 1, 1L);
    SchedulerTestSupport.enqueue(scheduler, flow, 2, 1L);
  }

  /**
   * Requests the entire depth.
   *
   * @param result actor result carrier
   */
  @Actor
  public void first(IIIII_Result result) {
    result.r1 = SchedulerTestSupport.dispatchCode(scheduler.capacityAvailable(2));
  }

  /**
   * Concurrently requests the entire depth.
   *
   * @param result actor result carrier
   */
  @Actor
  public void second(IIIII_Result result) {
    result.r2 = SchedulerTestSupport.dispatchCode(scheduler.capacityAvailable(2));
  }

  /**
   * Reports the final capacity state and dispatch count.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = snapshot.queuedJobs();
    result.r4 = snapshot.runningJobs();
    result.r5 = (int) snapshot.dispatchedTotal();
  }
}
