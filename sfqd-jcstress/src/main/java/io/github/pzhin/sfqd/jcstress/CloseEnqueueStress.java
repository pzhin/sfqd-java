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

/** Proves atomic inactive-flow close versus admission on the same capability. */
@JCStressTest
@Outcome(id = "1, 3, 0, 0, 0", expect = Expect.ACCEPTABLE, desc = "Close removed the registration first.")
@Outcome(id = "2, 1, 1, 1, 1", expect = Expect.ACCEPTABLE, desc = "Admission activated the flow first.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Close and admission did not form one legal history.")
@State
public class CloseEnqueueStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
  private final FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");

  /**
   * Attempts to close the idle flow.
   *
   * @param result actor result carrier
   */
  @Actor
  public void close(IIIII_Result result) {
    result.r1 = SchedulerTestSupport.closeCode(scheduler.closeFlow(flow));
  }

  /**
   * Attempts to activate the same flow.
   *
   * @param result actor result carrier
   */
  @Actor
  public void enqueue(IIIII_Result result) {
    result.r2 = SchedulerTestSupport.enqueueCode(scheduler.enqueue(flow, 1, new Object(), 1L));
  }

  /**
   * Reports final registration, queue, and admission counts.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = snapshot.registeredFlows();
    result.r4 = snapshot.queuedJobs();
    result.r5 = (int) snapshot.acceptedTotal();
  }
}
