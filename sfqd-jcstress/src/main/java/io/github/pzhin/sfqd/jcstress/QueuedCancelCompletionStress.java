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

/** Proves the bounded result semantics when cancellation races completion of a queued job. */
@JCStressTest
@Outcome(id = "1, 2, 0, 1, 0", expect = Expect.ACCEPTABLE, desc = "Completion observed the queued job first.")
@Outcome(id = "1, 3, 0, 1, 0", expect = Expect.ACCEPTABLE, desc = "Cancellation removed the job first.")
@Outcome(expect = Expect.FORBIDDEN, desc = "Queued cancellation and completion produced an illegal history.")
@State
public class QueuedCancelCompletionStress {
  private final SfqdScheduler<String, Integer, Object> scheduler =
      new SfqdScheduler<>(new SchedulerConfig(1, 1, 1));
  private final JobHandle job;

  /** Builds a stable queued-job fixture. */
  public QueuedCancelCompletionStress() {
    FlowHandle flow = SchedulerTestSupport.register(scheduler, "flow");
    job = SchedulerTestSupport.enqueue(scheduler, flow, 1, 1L);
  }

  /**
   * Cancels the queued job.
   *
   * @param result actor result carrier
   */
  @Actor
  public void cancel(IIIII_Result result) {
    result.r1 = SchedulerTestSupport.cancelCode(scheduler.cancel(job));
  }

  /**
   * Attempts to complete the same queued job.
   *
   * @param result actor result carrier
   */
  @Actor
  public void complete(IIIII_Result result) {
    result.r2 = SchedulerTestSupport.completionCode(scheduler.complete(job));
  }

  /**
   * Reports final queue and terminal counters.
   *
   * @param result actor and arbiter result carrier
   */
  @Arbiter
  public void report(IIIII_Result result) {
    SchedulerSnapshot snapshot = scheduler.snapshot();
    result.r3 = snapshot.queuedJobs();
    result.r4 = (int) snapshot.cancelledTotal();
    result.r5 = (int) snapshot.completedTotal();
  }
}
