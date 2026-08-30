# Theory behind SFQ(D)

This document explains the ideas behind the library without requiring the
reader to follow the proofs. The implementation contract is in
[FORMAL_SPEC.md](FORMAL_SPEC.md); the public API and examples are in the
[README](../README.md).

## The problem

Suppose several tenants share the same bounded execution capacity. A simple
FIFO queue ignores tenant weights and job sizes. Strict round-robin handles
tenants equally but behaves poorly when jobs have different costs. A scheduler
should instead charge each flow for the work it receives and let unused
capacity remain usable by others.

Fair queueing models each tenant as a **flow** with a positive weight. The
weight controls relative service when flows compete. A job has a positive
cost supplied by the application.

## Start-time Fair Queueing

Start-time Fair Queueing (SFQ) gives each job two virtual tags:

```text
S = max(V, previous finish tag of this flow)
F = S + cost / weight
```

`V` is scheduler virtual time. Jobs are selected by increasing start tag `S`.
The tags are accounting values, not wall-clock timestamps.

The practical effect is:

- a flow with twice the weight accumulates virtual cost at half the rate;
- larger jobs are charged more than smaller jobs;
- a flow may use otherwise idle capacity without being punished forever;
- scheduling remains meaningful when the real service rate changes.

The library uses exact reduced rational numbers for these tags. This avoids
floating-point comparison errors and makes tie behavior deterministic.

## Why the `(D)` matters

Classic SFQ describes one job in service. Real storage, RPC, database, and
worker systems often need several requests in flight to use their internal
parallelism.

SFQ(D) permits at most `D` jobs to be dispatched but not yet completed. It
still selects the smallest queued start tag, then sets `V` to the selected
job's start tag. Completion frees an issue slot; the caller may then ask the
scheduler for more work.

Increasing `D` can improve utilization, but it also permits more work to be
in flight before the scheduler can react. The published fairness discrepancy
bound therefore grows with `D + 1`.

`D` is an issue-depth limit, not automatically a count of physical resources.
For a direct pool of `N` identical non-preemptive workers, `D = N` is the usual
mapping. A system with its own admission or multiplexing layer may choose a
different mapping, but fairness then applies to the configured issue slots.

## What the papers support

Under continuously backlogged flows, fixed positive weights, bounded positive
job costs, and continued completion/dispatch progress, SFQ and SFQ(D) bound
the difference in normalized completed service between competing flows. The
bound is expressed in the same cost units supplied by the caller.

This does **not** automatically guarantee fairness in real elapsed time. If a
cost estimate poorly represents actual work, the scheduler is fair with
respect to the estimate, not the unknown true cost. The papers also do not
define Java handles, duplicate identifiers, cancellation, numeric overflow,
thread-safety, or retention rules.

## Engineering decisions in this library

The Java library makes the unspecified parts explicit:

- every public operation is linearizable;
- equal start tags use admission order as a stable FIFO tie-break;
- cancellation succeeds only before dispatch;
- charge-reserved cancellation is the default, while opt-in refund cancellation
  prospectively removes queued virtual cost from later work of the same flow;
- handles are opaque and scheduler-specific;
- counts, sequences, and exact-number sizes are bounded and fail closed;
- rejected operations are atomic no-ops;
- terminal jobs and payloads are not retained as tombstones;
- reaching global idle normalizes virtual and per-flow finish history to zero.

The last rule is an equivalent busy-period normalization described for SFQ.
It prevents old virtual debt from growing forever across periods with no live
work while preserving the ordering within each busy period.

### Cancellation policies and claims

Cancellation is a library extension rather than a result supplied by the SFQ
or SFQ(D) papers. The default policy keeps the tags assigned at admission, so a
cancelled queued cost remains virtual debt until global idle. The opt-in refund
policy instead recomputes the later queued suffix of that flow at the
cancellation linearization point. It does not undo an earlier dispatch or
rewrite virtual-time history.

Exact refund must not turn cancellation into a fallible cleanup operation. The
refund policy therefore narrows admission: before accepting a job, the library
proves that all tags reachable by removing any subset of the queued costs fit
the fixed exact-number budget. Positivity makes the full queued sum the largest
candidate numerator, and a common denominator covers every subset. Numeric
failure is reported by enqueue, while every accepted queued job remains safely
cancellable.

Neither cancellation extension automatically inherits the papers'
completed-work fairness bound. Charge-reserved debt is not completed service,
and prospective refund changes future tags without revising past scheduling
decisions. A claim for cancellation intervals would require a separate proof.

## Starvation and progress

The scheduler cannot make work finish and cannot guarantee progress when the
caller stops completing dispatched jobs or stops offering capacity. With
continued completion/dispatch calls, starvation of a queued head is excluded
under the library's bounded domain: finitely many registered flows and live
jobs, fixed positive weights, positive costs, stable FIFO tie-breaking, and
finite completion of every dispatched job. Each competing flow's virtual
finish value must advance, so it cannot place infinitely many later jobs ahead
of one fixed queued start tag.

## Related algorithms

- **SFQ** is the single-issue-depth foundation.
- **SFQ(D)** extends that accounting to a bounded number of outstanding jobs.
- **FlashFQ** studies practical fair queueing for flash storage and discusses
  deeper issue queues and implementation trade-offs.
- **Multi-Queue Fair Queuing** studies scalable fair queueing across multiple
  hardware queues; it is related context, not the algorithm implemented here.
- **MSFQ/MSF²Q** studies a different multi-server model based on approximating
  an aggregate GPS server. It should not be confused with SFQ(D).

## Full primary sources

1. P. Goyal, H. M. Vin, H. Cheng, **Start-time Fair Queueing: A
   Scheduling Algorithm for Integrated Services Packet Switching Networks**,
   SIGCOMM 1996. [Full PDF](https://conferences.sigcomm.org/sigcomm/1996/papers/goyal.pdf),
   [ACM DOI](https://doi.org/10.1145/248157.248171).
2. W. Jin, J. S. Chase, J. Kaur, **Interposed Proportional Sharing for a
   Storage Service Utility**, SIGMETRICS 2004. [Full
   PDF](https://www.cs.unc.edu/~jasleen/papers/sigmetrics04.pdf),
   [ACM DOI](https://doi.org/10.1145/1005686.1005694).
3. K. Shen, S. Park, **FlashFQ: A Fair Queueing I/O Scheduler for
   Flash-Based SSDs**, USENIX ATC 2013. [Full
   PDF](https://www.usenix.org/system/files/conference/atc13/atc13-shen.pdf),
   [USENIX page](https://www.usenix.org/conference/atc13/technical-sessions/presentation/shen).
4. M. Hedayati, K. Shen, M. L. Scott, M. Marty, **Multi-Queue Fair
   Queuing**, USENIX ATC 2019. [Full
   PDF](https://www.usenix.org/system/files/atc19-hedayati-fair-queuing.pdf),
   [USENIX page](https://www.usenix.org/conference/atc19/presentation/hedayati).
5. J. M. Blanquer, B. Özden, **Fair Queuing for Aggregated Multiple
   Links**, SIGCOMM 2001. [Full
   PDF](https://conferences.sigcomm.org/sigcomm/2001/p15-blanquer.pdf),
   [ACM DOI](https://doi.org/10.1145/383059.383074).
