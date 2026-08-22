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
import org.openjdk.jcstress.infra.results.IIIIIII_Result;

/** Proves that a snapshot observes an atomic depth-two batch wholly before or wholly after dispatch. */
@JCStressTest
@Outcome(id = "515, 2, 0, 2, 0, 0, 2", expect = Expect.ACCEPTABLE,
    desc = "Snapshot preceded the exact two-job batch.")
@Outcome(id = "515, 0, 2, 0, 2, 0, 2", expect = Expect.ACCEPTABLE,
    desc = "Snapshot followed the exact two-job batch.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Snapshot observed a torn dispatch batch.")
@State
public class SnapshotAtomicBatchStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(2, 1, 2));

  /** Builds a stable depth-two queued fixture. */
  public SnapshotAtomicBatchStress() {
    FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
    SchedulerTestSupport.enqueue(scheduler, flow, 1, 1L);
    SchedulerTestSupport.enqueue(scheduler, flow, 2, 1L);
  }

  /**
   * Dispatches both queued jobs atomically.
   *
   * @param result actor result carrier
   */
  @Actor
  public void dispatch(IIIIIII_Result result) {
    result.r1 = SchedulerTestSupport.dispatchCode(scheduler.capacityAvailable(2));
  }

  /**
   * Takes one aggregate snapshot.
   *
   * @param result actor result carrier
   */
  @Actor
  public void snapshot(IIIIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r2 = snapshot.queuedJobs();
    result.r3 = snapshot.runningJobs();
    result.r4 = snapshot.freeSlots();
    result.r5 = (int) snapshot.dispatchedTotal();
  }

  /**
   * Reports the final queue and running counts.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r6 = snapshot.queuedJobs();
    result.r7 = snapshot.runningJobs();
  }
}
