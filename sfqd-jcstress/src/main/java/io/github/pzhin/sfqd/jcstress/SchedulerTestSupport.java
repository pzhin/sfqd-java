package io.github.pzhin.sfqd.jcstress;

import io.github.pzhin.sfqd.CancelResult;
import io.github.pzhin.sfqd.CloseFlowResult;
import io.github.pzhin.sfqd.CompletionResult;
import io.github.pzhin.sfqd.Dispatch;
import io.github.pzhin.sfqd.EnqueueResult;
import io.github.pzhin.sfqd.FlowHandle;
import io.github.pzhin.sfqd.JobHandle;
import io.github.pzhin.sfqd.RegisterFlowResult;
import io.github.pzhin.sfqd.SfqdScheduler;
import java.util.List;

final class SchedulerTestSupport {
  static final int ACCEPTED = 1;
  static final int DUPLICATE_LIVE_ID = 2;
  static final int FLOW_NOT_REGISTERED = 3;
  static final int REGISTERED = 1;
  static final int FLOW_LIMIT = 2;
  static final int CANCELLED = 1;
  static final int TOO_LATE = 2;
  static final int NOT_LIVE = 3;
  static final int COMPLETED = 1;
  static final int NOT_DISPATCHED = 2;
  static final int CLOSED = 1;
  static final int FLOW_ACTIVE = 2;

  private SchedulerTestSupport() {}

  static FlowHandle register(SfqdScheduler<String, Integer, Object> scheduler, String flowId) {
    RegisterFlowResult result = scheduler.registerFlow(flowId, 1L);
    if (result instanceof RegisterFlowResult.Registered registered) {
      return registered.flowHandle();
    }
    throw new IllegalStateException("fixture flow registration was rejected");
  }

  static JobHandle enqueue(
      SfqdScheduler<String, Integer, Object> scheduler, FlowHandle flow, int jobId, long cost) {
    EnqueueResult result = scheduler.enqueue(flow, jobId, new Object(), cost);
    if (result instanceof EnqueueResult.Accepted accepted) {
      return accepted.jobHandle();
    }
    throw new IllegalStateException("fixture job admission was rejected");
  }

  static int enqueueCode(EnqueueResult result) {
    if (result instanceof EnqueueResult.Accepted) {
      return ACCEPTED;
    }
    return switch ((EnqueueResult.Rejected) result) {
      case DUPLICATE_LIVE_ID -> DUPLICATE_LIVE_ID;
      case FLOW_NOT_REGISTERED -> FLOW_NOT_REGISTERED;
      case LIVE_LIMIT -> 4;
      case SEQUENCE_EXHAUSTED -> 5;
      case NUMERIC_LIMIT -> 6;
    };
  }

  static int registerCode(RegisterFlowResult result) {
    if (result instanceof RegisterFlowResult.Registered) {
      return REGISTERED;
    }
    return switch ((RegisterFlowResult.Rejected) result) {
      case FLOW_LIMIT -> FLOW_LIMIT;
      case DUPLICATE_REGISTERED_ID -> 3;
      case FLOW_SEQUENCE_EXHAUSTED -> 4;
    };
  }

  static int cancelCode(CancelResult result) {
    return switch (result) {
      case CANCELLED -> CANCELLED;
      case TOO_LATE_ALREADY_DISPATCHED -> TOO_LATE;
      case NOT_LIVE -> NOT_LIVE;
    };
  }

  static int completionCode(CompletionResult result) {
    return switch (result) {
      case COMPLETED -> COMPLETED;
      case NOT_DISPATCHED -> NOT_DISPATCHED;
      case NOT_LIVE -> NOT_LIVE;
    };
  }

  static int closeCode(CloseFlowResult result) {
    return switch (result) {
      case CLOSED -> CLOSED;
      case FLOW_ACTIVE -> FLOW_ACTIVE;
      case FLOW_NOT_REGISTERED -> 3;
      case BUSY_PERIOD_ACTIVE -> 4;
    };
  }

  static int dispatchCode(List<Dispatch<String, Integer, Object>> dispatches) {
    int mask = 0;
    for (Dispatch<String, Integer, Object> dispatch : dispatches) {
      mask |= 1 << (dispatch.jobId() - 1);
    }
    return (dispatches.size() << 8) | mask;
  }
}
