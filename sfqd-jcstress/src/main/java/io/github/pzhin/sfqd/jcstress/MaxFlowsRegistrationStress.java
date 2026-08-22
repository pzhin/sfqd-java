package io.github.pzhin.sfqd.jcstress;

import io.github.pzhin.sfqd.SchedulerConfig;
import io.github.pzhin.sfqd.SfqdScheduler;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;

/** Proves atomic enforcement of the configured registration bound. */
@JCStressTest
@Outcome(id = "1, 2, 1", expect = Expect.ACCEPTABLE, desc = "First flow occupied the sole registration.")
@Outcome(id = "2, 1, 1", expect = Expect.ACCEPTABLE, desc = "Second flow occupied the sole registration.")
@Outcome(expect = Expect.FORBIDDEN, desc = "The maxFlows bound was not enforced atomically.")
@State
public class MaxFlowsRegistrationStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));

  /**
   * Registers the first identifier.
   *
   * @param result actor result carrier
   */
  @Actor
  public void first(III_Result result) {
    result.r1 = SchedulerTestSupport.registerCode(scheduler.registerFlow("first", 1L));
  }

  /**
   * Registers the competing identifier.
   *
   * @param result actor result carrier
   */
  @Actor
  public void second(III_Result result) {
    result.r2 = SchedulerTestSupport.registerCode(scheduler.registerFlow("second", 1L));
  }

  /**
   * Reports the final registration count.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(III_Result result) {
    result.r3 = scheduler.snapshot().registeredFlows();
  }
}
