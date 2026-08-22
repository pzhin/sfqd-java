package io.github.pzhin.sfqd.jcstress;

import io.github.pzhin.sfqd.EnqueueResult;
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

/** Proves that two admissions of the same live identifier cannot both succeed. */
@JCStressTest
@Outcome(id = "1, 2, 1, 1, 1", expect = Expect.ACCEPTABLE, desc = "First admission linearized first.")
@Outcome(id = "2, 1, 1, 2, 1", expect = Expect.ACCEPTABLE, desc = "Second admission linearized first.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Not a legal same-identifier admission history.")
@State
public class SameIdentifierEnqueueStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 1, 2));
  private final FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
  private final Object firstPayload = new Object();
  private final Object secondPayload = new Object();

  /**
   * Attempts the first admission.
   *
   * @param result actor result carrier
   */
  @Actor
  public void first(IIIII_Result result) {
    EnqueueResult outcome = scheduler.enqueue(flow, 7, firstPayload, 1L);
    result.r1 = SchedulerTestSupport.enqueueCode(outcome);
  }

  /**
   * Attempts the competing admission.
   *
   * @param result actor result carrier
   */
  @Actor
  public void second(IIIII_Result result) {
    EnqueueResult outcome = scheduler.enqueue(flow, 7, secondPayload, 1L);
    result.r2 = SchedulerTestSupport.enqueueCode(outcome);
  }

  /**
   * Reports the sole dispatch and aggregate admission count.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIII_Result result) {
    var dispatches = scheduler.capacityAvailable(1);
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = dispatches.size();
    if (dispatches.isEmpty()) {
      result.r4 = 0;
    } else if (dispatches.get(0).payload() == firstPayload) {
      result.r4 = 1;
    } else if (dispatches.get(0).payload() == secondPayload) {
      result.r4 = 2;
    } else {
      result.r4 = 3;
    }
    result.r5 = (int) snapshot.acceptedTotal();
  }
}
