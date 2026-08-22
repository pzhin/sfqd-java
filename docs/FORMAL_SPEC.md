# SFQ(D) Java Library — Formal Specification

## 1. Status, scope, and normative terms

Status: normative specification of the library's sequential behavior and the
linearization contract of the concurrent production API.

[THEORY.md](THEORY.md) provides a plain-language theoretical foundation and
links to the full papers. Every decision not defined by the cited literature is
marked as a **design decision**.

The terms **MUST**, **MUST NOT**, and **MAY** are normative. This document does
not freeze public Java type and method names; it freezes the observable
semantics of the public API and production implementation.

The specification covers one scheduler instance and these calls:

- `registerFlow`;
- `closeFlow`;
- `enqueue`;
- `cancel`;
- `dispatchUpTo`, also called `dispatch` below;
- `complete`;
- `snapshot()` and `snapshot(flowHandle)`.

The scheduler selects jobs but does not execute them, own an executor or
resource pool, or invoke callbacks.

The name `dispatchUpTo` intentionally describes an action: every non-empty
result has already moved jobs irreversibly to running and occupied issue slots.
The operation is not a notification that capacity has become available.

## 2. Model and configuration

### 2.1 External entities

**API and numeric design decision.** The supported input domain is:

- `FlowId` — any non-null caller object with equality defined by `equals`. Its
  `equals` and `hashCode` behavior MUST remain stable while the flow is
  registered.
- `FlowHandle` — an opaque capability for one particular flow registration.
  Enqueue accepts this handle, not a raw `FlowId` or weight.
- `JobId` — any non-null caller object with equality defined by `equals`. Its
  `equals` and `hashCode` behavior MUST remain stable while the job is live.
- `Payload` — any non-null caller object. The scheduler does not interpret it.
- `cost` — the supplied job cost, an integer in `1..Long.MAX_VALUE`.
- `weight` — the flow weight, an integer in `1..Long.MAX_VALUE`.

`FlowId` and `JobId` MUST honor the Java `equals/hashCode` contract throughout
the stated lifetime. Both methods MUST be deterministic, side-effect-free, and
non-reentrant with respect to the scheduler; they MUST NOT invoke scheduler
operations or throw exceptions. This is a caller precondition, not a detectable
operational outcome: after an object enters a hash-based index, the library
cannot reliably detect a violation. If the precondition is violated, identity
and linearizability guarantees do not apply. The implementation MUST NOT claim
that it can atomically reject a mutable or throwing key after using that key.

Fairness is defined in terms of `cost`, not unknown actual execution time.

### 2.2 Immutable instance configuration

**Configuration design decision.**

- `D` — the number of issue slots, an integer in `1..1_000_000`.
- `maxFlows` — the maximum number of simultaneously registered flows, an
  integer in `1..Integer.MAX_VALUE`.
- `maxLiveJobs` — the explicit `queued + dispatched` limit, an integer in
  `D..Integer.MAX_VALUE`.
- `cancellationAccounting` — the fixed policy
  `CancellationAccounting.CHARGE_RESERVED_COST`; no alternative policy exists.

All four values are immutable after instance construction. Null values,
out-of-range values, and `maxLiveJobs < D` are rejected before an observable
instance exists.

The upper bound `1_000_000` defines the supported representation and
configuration validation. It does not claim that an atomic batch of this size
has acceptable latency, throughput, allocation rate, or internal serialization
hold time. The repository's measurement matrix is limited to `D <= 1_024` and,
without a preserved and verified run, is not itself a performance result.

### 2.3 Mapping D to N resources

In Jin04, `D` is the maximum number of dispatched-but-not-completed requests,
not the number of physical resources; see
[Why the `(D)` matters](THEORY.md#why-the-d-matters).

**Design decision.** The scheduler does not configure `N` separately. For a
model with `N` identical parallel non-preemptive resources, where every
returned job immediately occupies one resource, the caller MUST set `D = N`
and call `dispatchUpTo(k)` only when it can accept up to `k` jobs.

`D != N` is allowed for a black-box service with an internal queue or admission
depth limit, but then:

- the fairness bound uses the configured `D`;
- the library guarantees work conservation of issue slots, not of a physical
  device;
- physical utilization and the time at which a job actually starts are outside
  the contract.

The `k` argument of `dispatchUpTo(k)` is the maximum number of results from that
call, not a retained permit and not a change to `D`.

## 3. Exact numbers and limits

### 3.1 Semantic representation

A tag is an exact non-negative rational number `n/d`, where:

- `n` is a non-negative `BigInteger`;
- `d` is a positive `BigInteger`;
- `gcd(n,d)=1`;
- zero has the canonical representation `0/1`.

Addition, `max`, and comparison are mathematically exact. Comparing `a/b` and
`c/d` uses the sign of `a*d - c*b`; floating point, decimal rounding, and
approximate division are forbidden. Intermediate products are also computed
without fixed-width overflow.

For an accepted job, the normalized increment is the exact fraction

```text
increment = cost / weight.
```

### 3.2 Numeric budget

**Design decision.** After canonical reduction, the numerator and denominator
of every stored tag MUST have `bitLength <= 4096`. This is part of a fail-closed
engineering budget, not an approximation and not a promise to accept every
mathematically valid trace made from syntactically valid `long` inputs.
`NUMERIC_LIMIT` MAY occur for valid `cost` and `weight` values if the exact
history has already produced a fraction that is too complex. This is an
explicit production-representation boundary; the mathematical
unbounded-rational oracle does not have it.

The persistent budget applies to `V`, the `S/F` tags of queued jobs, and the
`lastFinish` of every registered flow. An exact primitive over two persistent
values MAY temporarily produce a numerator, denominator, or cross-product of
up to 8193 bits; this is `MAX_TRANSIENT_BITS`.

The transient budget is defined by mathematical quantities, not by internal
`BigInteger` objects of a particular algorithm. For reduced `a/b` and `c/d`,
the canonical quantities of a primitive are:

```text
add raw numerator       = a*d + c*b
subtract raw numerator  = abs(a*d - c*b)
raw denominator         = b*d
comparison products     = a*d and c*b
rebase subtraction      = the same subtract numerator and denominator
reduced result          = raw numerator/raw denominator after gcd reduction
```

The bit length of every listed raw or reduced quantity MUST be at most 8193
during a primitive; a persistent result after reduction MUST additionally fit
within 4096 bits. An implementation MAY use gcd-before-multiply,
cross-cancellation, or another exact algorithm, but it accepts or rejects the
operation as if these canonical mathematical quantities had been checked. The
size of an incidental implementation temporary object is not an API outcome
and cannot by itself cause `NUMERIC_LIMIT`.

Canonical trigger: only when an initially calculated new `S` or `F` does not
fit the persistent budget, `enqueue` MUST calculate the exact rebase from §3.3
exactly once on a temporary copy and retry the calculation. If the rebase or
retry does not fit the transient budget, or if any final stored value does not
fit the persistent budget, the operation returns `NUMERIC_LIMIT` and leaves all
observable state unchanged. Otherwise, the rebase and enqueue commit as one
transaction. Silent overflow, rounding, partial rebase, and order changes are
forbidden.

When `V=0` and `lastFinish=0`, a registered flow MUST accept one job with any
`cost,weight` pair in `1..Long.MAX_VALUE`, including the maximum values, unless
an independent identity or live-item limit applies: a single reduced fraction
occupies at most 63 bits in either component.

The number of successfully accepted jobs over an instance lifetime is limited
to `Long.MAX_VALUE`. State stores `lastJobSequence` in
`0..Long.MAX_VALUE`; a new handle receives `lastJobSequence + 1` only when
`lastJobSequence < Long.MAX_VALUE`. After sequence `Long.MAX_VALUE` has been
issued, later enqueue calls return `SEQUENCE_EXHAUSTED`; overflowing addition is
never performed and a sequence is never reused. Failed and no-op calls do not
consume a sequence.

Flow registrations use an independent `lastFlowSequence` with the same
`0..Long.MAX_VALUE` rules. After exhaustion, `registerFlow` returns
`FLOW_SEQUENCE_EXHAUSTED`; a closed FlowHandle is never reused.

Summary of cardinality and lifetime ranges:

```text
running jobs       0..D
queued jobs        0..maxLiveJobs
all live jobs      0..maxLiveJobs
active flows       0..min(maxLiveJobs,maxFlows)
registered flows   0..maxFlows
accepted jobs      0..Long.MAX_VALUE per scheduler instance
flow registrations 0..Long.MAX_VALUE per scheduler instance
stored tag bits    0..4096 per numerator or denominator
transient tag bits 0..8193 per exact primitive component
```

The number of read-only and failed calls is not limited by scheduler state. The
number of successful cancel, dispatch, and complete calls is limited by the
number of accepted incarnations.

### 3.3 Exact rebasing

A rebase is a representational substitution of the same semantic state. Let
`B = V` before the rebase. The following then happen atomically:

1. for every queued job: `S := S-B`, `F := F-B`;
2. for every registered flow, including inactive flows:
   `lastFinish := max(0, lastFinish-B)`;
3. `V := 0`.

The `S >= V` invariant for queued jobs guarantees non-negativity. The scheduler
does not retain the tags of already dispatched jobs, so they require no
transformation. The transformation preserves all future `max` operations,
increments, comparisons, and tie order.

A rebase MUST NOT be partial. It runs only under the canonical trigger in §3.2
and within the same atomic state transition as the operation that caused it.
Its worst-case time and temporary space are
`O(queuedJobs + registeredFlows)` exact rational components. If any transformed
value violates the persistent or transient budget, the temporary copy is
discarded. Proactive, partial, or repeated rebasing is forbidden because the
timing of normalization must not change the observable point at which
`NUMERIC_LIMIT` occurs across implementations.

### 3.4 New busy period

**Design decision based on the normalization permitted by Goyal96**, described
under [Engineering decisions](THEORY.md#engineering-decisions-in-this-library).
When the set of live jobs becomes empty after `cancel` or `complete`, the same
atomic transition performs:

- `V := 0`;
- `lastFinish := 0` for every registered flow;
- registration records, numeric sequences, and cumulative counters are **not**
  reset.

This removes tag debt across busy periods and is the selected resolution of
plain SFQ(D) described under
[Why the `(D)` matters](THEORY.md#why-the-d-matters).

## 4. Abstract state

Scheduler state is the tuple:

```text
Config            = (D, maxFlows, maxLiveJobs)
ownerToken        = inert identity token of this instance
V                 = virtual-time tag
lastJobSequence   = last issued job long sequence, initially 0
lastFlowSequence  = last issued flow long sequence, initially 0
RegisteredById    = FlowId -> FlowHandle
RegisteredFlows   = FlowHandle -> FlowState
LiveById          = JobId -> JobHandle
Queued            = JobHandle -> QueuedJob
Running           = JobHandle -> RunningJob
Priority          = total order of queued jobs by (S, jobSequence)
Counters          = accepted, dispatched, cancelled, completed
```

In the initial state, both sequences and all counters are zero, `V=0`, all maps
and sets are empty, the owner token exists, and configuration has already been
validated.

`FlowHandle` and `JobHandle` are normatively **inert capabilities**. Each
contains only a small `ownerToken` and the corresponding never-reused
`long sequence`. A handle MUST NOT contain a reference or back-reference to the
scheduler, `FlowId`, `JobId`, payload, map, record, or any other caller-domain
object. The `ownerToken` itself is a separate immutable identity marker with no
fields referring to the scheduler or its state. The scheduler and its handles
may refer to the marker, but the marker refers to none of them.

A handle from another instance has a different owner token. A closed or
terminal handle remains a safe inert value but no longer resolves to any live
record. A returned `Dispatch` contains the payload and IDs for the caller; it is
a caller-owned result, not handle content or a stored terminal record.

Normative equality for both handle types is:

- two handles are equal only if they have the same runtime type, their
  `ownerToken` is the same object by identity (`==`), and their sequences are
  equal;
- `hashCode` is stable and derived only from the identity token and sequence;
- a `FlowHandle` is never equal to a `JobHandle`, even with the same numeric
  sequence;
- handle types MUST NOT implement `Comparable` or `Serializable`;
- the public API MUST NOT expose token or sequence accessors.

Callers can therefore safely use a handle as an opaque map key, but cannot
derive a global order, transfer the capability between processes or JVMs, or
construct it from a numeric ID.

```text
FlowState = (flowHandle, flowId, weight, lastFinish,
             queuedCount, runningCount)

QueuedJob = (jobHandle, jobId, flowHandle, payload,
             cost, S, F, jobSequence)

RunningJob = (jobHandle, jobId, flowHandle, cost)
```

The production implementation MAY store equivalent state in other structures.
The reference model MUST prefer a direct representation over optimization.

### 4.1 Flow-state definitions

- a flow is `active` when `queuedCount + runningCount > 0`;
- a flow is `backlogged` when `queuedCount > 0`;
- a flow is `inactive` when it is registered and both counts are zero;
- an active non-backlogged flow has only running jobs.

Registration and activity are orthogonal. Flow state and `lastFinish` persist
across active → inactive transitions within a non-empty busy period. Weight is
immutable for the full registration lifetime, including inactive intervals. A
weight change is allowed only through a successful debt-safe `closeFlow`,
followed by a new `registerFlow` with a new FlowHandle.

### 4.2 Invariants

In every observable state:

1. `Queued` and `Running` do not overlap by handle.
2. Every handle is in at most one lifecycle state.
3. `LiveById` corresponds bijectively to `Queued union Running` by `JobId`.
4. `RegisteredById` and `RegisteredFlows` are bijective by
   `FlowId/FlowHandle`; `|RegisteredFlows| <= maxFlows`.
5. Every queued or running job refers to exactly one registered flow.
6. `|Queued| + |Running| <= maxLiveJobs`.
7. `|Running| <= D`; `freeSlots = D - |Running|`.
8. Flow counters equal the number of corresponding records; active and
   backlogged are derived only from the counters.
9. For every queued job, `S >= V`.
10. `Priority` contains exactly `Queued` and is ordered as defined in §6.
11. `V`, queued tags, and `lastFinish` for all registered flows are canonical,
    exact, and within the persistent numeric budget.
12. A payload is stored only in `Queued`, never in `Running`, a handle, or
    terminal state.
13. Cumulative lifecycle conservation holds with mathematical exactness:
    `accepted = |Queued| + |Running| + cancelled + completed` and
    `dispatched = |Running| + completed`.
14. No counter exceeds `Long.MAX_VALUE`; equality and sum checks MUST NOT use
    overflowing fixed-width arithmetic.

## 5. Lifecycles and identity

The flow-registration lifecycle is:

```text
ABSENT --registerFlow/REGISTERED--> REGISTERED_INACTIVE
REGISTERED_INACTIVE --enqueue-----> REGISTERED_ACTIVE
REGISTERED_ACTIVE --last terminal-> REGISTERED_INACTIVE
REGISTERED_INACTIVE --closeFlow---> ABSENT
```

`closeFlow` is allowed only for an inactive flow with `lastFinish <= V`. The
preserved identity and a new registration would then give the next job the same
start tag `V`, so removal does not erase outstanding fairness debt. The
condition can become true after a global idle reset or within a busy period.

The lifecycle of one JobHandle is:

```text
ABSENT --enqueue/ACCEPTED--> QUEUED
QUEUED --dispatch----------> RUNNING
QUEUED --cancel/CANCELLED--> ABSENT
RUNNING --complete---------> ABSENT
```

There are no other transitions. Dispatch and cancel are irreversible. A
running job is non-preemptive from the scheduler's perspective; `cancel` does
not recall it.

### 5.1 Bounded duplicate semantics

**Design decision.** Terminal tombstones are not stored. After a handle is
removed, the library intentionally does not distinguish among:

- a handle that never existed;
- an already cancelled handle;
- an already completed handle;
- a repeated terminal-operation call.

All return `NOT_LIVE`. This is not a temporary cache, and the result does not
depend on the age of the call.

A late `cancel/NOT_LIVE` therefore intentionally does **not** identify the
terminal cause: it cannot distinguish a job completed after dispatch from a
previously cancelled, stale, foreign, or never-existing handle. The winner of
cancel versus dispatch is determined from the combined linearized history: the
presence of the handle in a dispatch result and the cancel result. While a
selected job remains running, an immediate cancel returns
`TOO_LATE_ALREADY_DISPATCHED`; after completion, that diagnostic information is
removed with the bounded terminal metadata.

A caller `JobId` is unique only among live jobs. While the ID is live, another
`enqueue` returns `DUPLICATE_LIVE_ID`. After a terminal transition, the same ID
may be accepted as a new incarnation and receives a new handle. All lifecycle
operations accept a handle, not merely a `JobId`; a delayed call with an old
handle therefore cannot affect the new incarnation (no ABA).

Consequently, repeating `enqueue` after a terminal transition is **not** an
idempotent retry: it is a new job. If a caller requires indefinite business-
request idempotency, it stores that state outside the scheduler.

These semantics provide at-most-once behavior without unbounded completed or
cancelled metadata. The API MUST NOT promise a distinct `ALREADY_COMPLETED` or
`ALREADY_CANCELLED` result because it cannot prove one without retention.

A caller `FlowId` is unique among registered flows. While a registration
exists, another `registerFlow` returns `DUPLICATE_REGISTERED_ID`. After a safe
`closeFlow`, the same ID may receive a new registration and a new FlowHandle. A
stale or foreign FlowHandle returns `FLOW_NOT_REGISTERED` and cannot address a
new registration of the same ID.

## 6. Tags, virtual time, and total order

### 6.1 Enqueue tags

For an accepted job `j` of flow `f`:

```text
previousFinish = RegisteredFlows[f].lastFinish
S(j) = max(V, previousFinish)
F(j) = S(j) + cost(j) / RegisteredFlows[f].weight
```

After acceptance, `RegisteredFlows[f].lastFinish := F(j)`. Enqueue does not
create a registration and does not accept a weight. A dormant registered flow
uses its stored `lastFinish`, not zero; an inactive interval within a busy
period therefore does not reset fairness history.

Tags are fixed at enqueue. Later cancellation of another queued job MUST NOT
recalculate the tags of remaining jobs or roll back `lastFinish`.

This last rule is a **cancellation design decision** absent from Jin04. It
prevents retroactive changes to accepted scheduling decisions. Its cost is that
a cancelled supplied cost remains a virtual charge until the end of the current
global busy period; the published completed-work fairness bound is not claimed
for intervals containing cancellation.

### 6.2 Deterministic tie-breaking

**Design decision.** The priority key of a queued job is:

```text
(S ascending, jobSequence ascending)
```

`jobSequence` is assigned in the linearization order of successful enqueue
calls and is unique. This creates a complete deterministic order. Jin04 permits
arbitrary tie-breaking; this rule selects one permitted ordering.

### 6.3 Dispatch and virtual time

Each selected job is the minimum in the current `Priority`. Immediately before
it transitions to running:

```text
V := S(selected)
```

During batch dispatch, jobs are selected sequentially; the updated `V` applies
to the next selection. Because queued order is non-decreasing by `S`, `V` does
not decrease within a busy period and may remain unchanged.

`complete` does not itself advance `V`; it only frees an issue slot. `V`
changes on dispatch, exact rebase, or reset at a busy-period boundary.

## 7. Operations and results

All checks, calculations, and changes within one operation are logically
atomic. Invalid arguments are rejected before state mutation. A concrete Java
API MAY represent programmer errors with exceptions and the listed operational
outcomes with result values, but JavaDoc MUST document that distinction
consistently.

### 7.1 `registerFlow(flowId, weight)`

Order of processing:

1. Validate a non-null `flowId` and `weight` in `1..Long.MAX_VALUE`.
2. If `flowId` is in `RegisteredById`, return
   `DUPLICATE_REGISTERED_ID`.
3. If the registered count equals `maxFlows`, return `FLOW_LIMIT`.
4. If `lastFlowSequence == Long.MAX_VALUE`, return
   `FLOW_SEQUENCE_EXHAUSTED`.
5. Create an inert `FlowHandle(ownerToken,lastFlowSequence+1)` and FlowState
   with `lastFinish=0`, zero counts, and the fixed weight; insert both
   registration indexes and update the sequence.
6. Return `REGISTERED(flowHandle)`.

Registration during a non-empty busy period is allowed: it does not affect
scheduling before the first enqueue. A rejection leaves state unchanged and
does not consume a sequence.

### 7.2 `closeFlow(flowHandle)`

A null handle is an invalid argument. For a foreign, stale, or already closed
handle, return `FLOW_NOT_REGISTERED`.

- If the registered flow is active, return `FLOW_ACTIVE`.
- If the flow is inactive but `lastFinish > V`, return
  `FAIRNESS_DEBT_ACTIVE`.
- If the flow is inactive and `lastFinish <= V`, atomically remove it from
  `RegisteredFlows` and `RegisteredById`, release the internal `FlowId`
  reference, and return `CLOSED`.

The closing condition is fairness-neutral: with the old identity preserved,
the next enqueue would receive `S=max(V,lastFinish)=V`; a new registration with
`lastFinish=0` also receives `S=V`. The global idle reset in §3.4 is a
sufficient but not necessary way to reach the condition. Success permits the
same `FlowId` to be registered again, but the new FlowHandle receives a new
sequence.

### 7.3 `enqueue(flowHandle, jobId, payload, cost)`

Order of processing:

1. Validate a non-null handle, ID, and payload, and validate the `cost` range.
2. If the FlowHandle is foreign, stale, or closed, return
   `FLOW_NOT_REGISTERED`.
3. If `jobId` is in `LiveById`, return `DUPLICATE_LIVE_ID`.
4. If the live count equals `maxLiveJobs`, return `LIVE_LIMIT`.
5. If the job sequence is exhausted, return `SEQUENCE_EXHAUSTED`.
6. Using the fixed registered weight and `lastFinish`, calculate exact `S` and
   `F`; when required, apply §3.3 transactionally to the complete necessary
   state copy. If the budget remains violated, return `NUMERIC_LIMIT`.
7. Create the inert JobHandle and queued record, insert it into all job indexes,
   and update the registered flow counts, `lastFinish`, counters, and job
   sequence.
8. Return `ACCEPTED(jobHandle)`.

Every result other than `ACCEPTED` leaves observable state unchanged and does
not consume a sequence.

### 7.4 `cancel(handle)`

A null handle is an invalid argument. An opaque handle from another scheduler
instance is treated as `NOT_LIVE`.

- If the handle is in `Queued`, atomically remove the job from the queue,
  priority, and `LiveById`; decrement the flow count, increment `cancelled`,
  release the payload, and return `CANCELLED`.
- If the handle is in `Running`, change nothing and return
  `TOO_LATE_ALREADY_DISPATCHED`.
- Otherwise, return `NOT_LIVE`.

JavaDoc MUST state explicitly that `NOT_LIVE` proves only the absence of a live
job at the linearization point and does not by itself prove which terminal
operation occurred. If the caller needs the cause, it correlates this result
with earlier dispatch, cancel, and completion results.

Registered flow state persists on deactivation. If the removed job was the
scheduler's last live job, the same transition performs the §3.4 reset for all
registrations. Cancellation does not return capacity because a queued job did
not occupy any.

### 7.5 `dispatchUpTo(k)` / `dispatch(k)`

`k` is an integer in `0..D`; a negative value or `k>D` is an invalid argument.
`k=0` returns an empty list without mutation.

The public Java API uses the action-oriented name `dispatchUpTo` because a
non-empty call changes job lifecycles. Names resembling capacity notifications
do not match these semantics.

The number of selected jobs is:

```text
m = min(k, D - |Running|, |Queued|).
```

For `i=1..m`, the operation sequentially:

1. takes the minimum of `Priority`;
2. sets `V := S(job)`;
3. removes the queued record and payload from internal queued structures;
4. creates a `RunningJob`, retaining the handle, IDs, flow, and cost, but not
   the payload;
5. updates flow counts and `dispatched`;
6. appends `Dispatch(handle, jobId, flowId, payload, cost)` to the result list.

The result list is ordered by actual dispatch order. The entire batch is one
atomic operation: no other call can interleave between its elements. If `m=0`,
the operation returns an empty list.

Each `Dispatch` is an immutable detached carrier for
`(jobHandle, jobId, flowId, payload, cost)`, but intentionally retains ordinary
Object identity equality and hash code and is **not** a value record: payload
has no `equals/hashCode` precondition, so structural equality would create a
false API contract. The returned list is also immutable or unmodifiable and
detached from internal collections; the scheduler never changes the carrier or
list after returning it.

Differential comparison checks carrier fields explicitly: corresponding opaque
handles through a logical mapping, IDs according to their contract, `cost`
numerically, and payload only by object identity (`==`) for the same input
trace. `Dispatch` objects and result lists are not compared through value
`equals`.

Each result immediately and irreversibly consumes one issue slot until a
successful `complete`. A repeated `dispatchUpTo` cannot issue the same slot or
job. The caller MUST invoke the operation only when ready to accept the entire
returned batch. Caller or executor failure after return does not roll back the
dispatch; the caller must still finish the handle through `complete`. Requeue
is a new incarnation through a new enqueue and is not part of this operation.

### 7.6 `complete(handle)`

A null handle is an invalid argument. An opaque handle from another scheduler
instance is treated as `NOT_LIVE`.

- If the handle is in `Running`, atomically remove the running record and
  `LiveById`, decrement the flow count, increment `completed`, release one
  internal issue slot, and return `COMPLETED`.
- If the handle is in `Queued`, change nothing and return `NOT_DISPATCHED`.
- Otherwise, return `NOT_LIVE`.

Registered flow state persists on deactivation. Removing the last live job
performs the §3.4 reset for all registrations. Completion does **not** dispatch
the next job automatically; the caller invokes `dispatchUpTo` separately.

Pull-based integration normally implements an external pump. To avoid leaving
available work or a resource without another dispatch attempt, the caller runs
the pump at least after every successful enqueue, every completion, and every
external capacity event. The scheduler still invokes no callback, and a
returned `Dispatch` contains no reference back to the scheduler.

### 7.7 `snapshot()`

A snapshot contains at least:

```text
D, maxFlows, maxLiveJobs,
registeredFlows,
queuedJobs, runningJobs, freeSlots,
activeFlows, backloggedFlows,
acceptedTotal, dispatchedTotal, cancelledTotal, completedTotal
```

A snapshot contains no payload, identifiers, handles, or internal tags. It is
an exact immutable atomic snapshot from one linearization point, not a weakly
consistent iteration. Cumulative counters exclude failed and no-op outcomes.

### 7.8 `snapshot(flowHandle)`

A null handle is an invalid argument. For the exact capability of a current
registration, the operation returns an immutable `FlowSnapshot`:

```text
queuedJobs, runningJobs,
acceptedCost, dispatchedCost, cancelledCost
```

Job counts describe current state. Cost totals are exact non-negative integer
sums of supplied cost over the lifetime of this registration:

- `acceptedCost` increases only on successful enqueue;
- `dispatchedCost` increases for each job in a successful dispatch batch;
- `cancelledCost` increases only on successful queued cancel;
- current `queuedCost = acceptedCost - dispatchedCost - cancelledCost`.

Completion does not change cost totals: `dispatchedCost` includes running and
completed jobs. Failed and no-op outcomes do not change the snapshot. Cost
totals do **not** overflow: exact integers represent them, and the global
never-reused job sequence bounds each value by
`Long.MAX_VALUE * Long.MAX_VALUE`.

For a foreign, stale, or closed capability, the operation returns empty. The
snapshot contains no FlowId, handle, payload, weight, internal tags, or
clock-derived age. The scheduler owns no clock and invokes no user metrics
callbacks. The caller knows the current weight from successful registration;
when needed, enqueue time or oldest age remains in the caller payload or an
external observer.

## 8. Linearization points and races

Public operations are fully thread-safe and linearizable. A particular lock or
CAS is not contractual; a linearization point (LP) is the abstract instant of
atomic commit or observation:

| Operation and result | Linearization point |
|---|---|
| `registerFlow/REGISTERED` | Atomic commit of both registration indexes and the flow sequence |
| `registerFlow/*rejection*` | Atomic observation of the first applicable registration check; state is unchanged |
| `closeFlow/CLOSED` | Atomic removal from `RegisteredFlows` and `RegisteredById` |
| `closeFlow/FLOW_ACTIVE` | Atomic observation of a non-zero flow job count |
| `closeFlow/FAIRNESS_DEBT_ACTIVE` | Atomic observation of an inactive flow with `lastFinish > V` |
| `closeFlow/FLOW_NOT_REGISTERED` | Atomic observation that the exact capability is absent from the registry |
| `enqueue/ACCEPTED` | Atomic commit of job insertion, flow and tag updates, sequence, and counter |
| `enqueue/*rejection*` | Atomic observation at which the first applicable result check holds; state is unchanged |
| `cancel/CANCELLED` | Atomic removal from `Queued` and `LiveById`, including flow, reset, and counter updates |
| `cancel/TOO_LATE_ALREADY_DISPATCHED` | Atomic observation of the handle in `Running` |
| `cancel/NOT_LIVE` | Atomic observation that the handle is absent from `Queued` and `Running` |
| `dispatch/non-empty` | One atomic commit of all `m` transitions and the final value of `V` |
| `dispatch/empty` | Atomic observation of `k=0`, or the simultaneous absence of a slot and/or queued job |
| `complete/COMPLETED` | Atomic removal from `Running` and `LiveById`, including slot, flow, reset, and counter updates |
| `complete/NOT_DISPATCHED` | Atomic observation of the handle in `Queued` |
| `complete/NOT_LIVE` | Atomic observation that the handle is absent from `Queued` and `Running` |
| `snapshot` | Atomic capture of all listed fields |
| `snapshot(flowHandle)` | Atomic registration lookup and capture of all flow fields, or observation of its absence |

Rebase, busy-period reset, and payload release are part of the LP of the
operation that caused them, not separate public events.

### 8.1 Required race outcomes

- `cancel` versus batch `dispatch`: if the cancel LP precedes the batch LP,
  cancel returns `CANCELLED` and the handle is absent from the dispatch result.
  If the batch LP precedes it **and this handle is selected**, the handle is
  returned exactly once and cancel returns `TOO_LATE_ALREADY_DISPATCHED`, or a
  late `NOT_LIVE` after completion. If an earlier batch selected only other
  jobs and left this handle in `Queued`, a later cancel MAY return `CANCELLED`.
  The mere fact of an earlier dispatch LP does not block cancellation of an
  unselected job.
- Two `dispatch` calls: batches are totally ordered by LP; capacity and jobs are
  not duplicated between them.
- Two `complete` calls: exactly one may return `COMPLETED`; the others return
  `NOT_LIVE`.
- Two `cancel` calls: exactly one may return `CANCELLED`; the others return
  `NOT_LIVE`.
- `complete` versus `cancel` of a queued job: cancel may return `CANCELLED`, and
  completion returns `NOT_DISPATCHED` before it or `NOT_LIVE` after it.
  Completion of a queued job can never succeed.
- `enqueue` of the same live `JobId`: exactly one may receive `ACCEPTED`; the
  others see `DUPLICATE_LIVE_ID` while the first incarnation remains live.
- `completion + dispatch`: if the completion LP comes first, the freed slot is
  available to the batch; otherwise dispatch does not use it and the caller
  must call again.
- `closeFlow + enqueue` for one FlowHandle: if the `enqueue/ACCEPTED` LP comes
  first, close observes an active flow and returns `FLOW_ACTIVE`; if the
  `closeFlow/CLOSED` LP comes first, enqueue returns `FLOW_NOT_REGISTERED`. A
  rejected enqueue (`DUPLICATE_LIVE_ID`, `LIVE_LIMIT`, `SEQUENCE_EXHAUSTED`,
  `NUMERIC_LIMIT`, or another rejection) is an atomic no-op: if its LP comes
  before close, close computes `CLOSED`, `FAIRNESS_DEBT_ACTIVE`, or
  `FLOW_ACTIVE` from the unchanged preceding state. A job cannot refer to a
  removed flow.
- `closeFlow` of an inactive flow versus the last completion or cancel of
  another flow: if `lastFinish > V` before the terminal LP, close returns
  `FAIRNESS_DEBT_ACTIVE`; if the terminal LP comes first, it performs the global
  reset and close may then return `CLOSED`. If debt is already repaid, close may
  return `CLOSED` before the terminal LP.
- Concurrent registrations at `maxFlows` cannot jointly exceed the limit; LP
  order gives the excess call `FLOW_LIMIT`. Registering the same `FlowId`
  against closing the old registration yields either
  `DUPLICATE_REGISTERED_ID` or a new distinct FlowHandle after `CLOSED`.
- `closeFlow` versus an enqueue requiring rebase: rebase exists only as part of
  the `enqueue/ACCEPTED` LP. Enqueue and rebase first activate the flow, after
  which close returns `FLOW_ACTIVE`. Close first returns `CLOSED` when
  `lastFinish <= V`, and a later enqueue returns `FLOW_NOT_REGISTERED`; when
  `lastFinish > V`, close returns `FAIRNESS_DEBT_ACTIVE`, preserves the
  registration, and enqueue can still perform the rebase atomically.
  `enqueue/NUMERIC_LIMIT` discards the entire temporary copy, so a later close
  is decided from the unchanged comparison of `lastFinish` and `V`. A rebase
  caused by another flow transforms both values by the common normalization in
  §3.3 and preserves the truth of the debt-safe condition.

These rules produce a history equivalent to some valid sequential execution.
The cancel-versus-dispatch winner is reconstructed from combined results and
history: `CANCELLED` plus absence of the handle from the batch means cancel won;
presence of the handle in the batch means dispatch won. One late `NOT_LIVE`
without history intentionally does not reveal the cause of absence. Public
JavaDoc for cancel and dispatch MUST describe this combined winner contract and
MUST NOT promise a cause from a single `NOT_LIVE`.

## 9. Activation, deactivation, and cancellation charge

### 9.1 Inactive → active

A successful enqueue of a registered inactive flow uses
`S=max(V,lastFinish)`. The registration already exists; activity changes only
because of the job count. Within a non-empty busy period, a dormant flow
preserves its finish history and cannot reset it through brief inactivity. If
the scheduler was completely idle, the §3.4 reset has already set `V=0` and all
registered `lastFinish=0`, starting a new normalized busy period.

### 9.2 Backlogged → active non-backlogged

Dispatching a flow's last queued job makes it non-backlogged if at least one
running job remains. State, weight, and `lastFinish` persist. A new enqueue
before the last completion uses this `lastFinish`.

### 9.3 Active → inactive

After a cancel or complete that reduces both flow counters to zero, the
registration, weight, and `lastFinish` persist. A later enqueue with the same
FlowHandle in this busy period uses `max(V,lastFinish)`. While
`lastFinish > V`, the caller cannot change weight or obtain a new fairness
identity. Once `V` reaches `lastFinish`, the registration can be closed safely
without changing the start tag of the next possible job.

The virtual charge of a cancelled job persists even after deactivation until
the global idle reset. This is the intentional non-retroactive semantics of
§6.1.

### 9.4 Registered → closed

Inactive does not mean closed. `closeFlow` removes identity only when
`lastFinish <= V`. A close followed by register, including registration with a
different weight, therefore does not reduce the next job's start tag: it equals
`V` before and after the operation.

## 10. Guarantees and claim boundaries

For traces without cancellation, the core conforms to plain SFQ(D) from Jin04
§3.2:

- `S=max(V,F_previous)` and `F=S+cost/weight`;
- dispatch in non-decreasing start-tag order;
- `V` equals the start tag of the last dispatch;
- at most `D` jobs are running simultaneously;
- `D=1` yields SFQ under the same tie rule; a flow registration cannot be
  replaced while its `lastFinish > V`;
- with queued work, positive `k`, and a free slot, dispatch returns a job;
- one backlogged flow may occupy all `D` slots.

The published pairwise completed-work bound from
[What the papers support](THEORY.md#what-the-papers-support) applies only under
its preconditions, including continuous backlog for both flows, positive fixed
weights, finite per-flow maximum costs, and a publication-compatible trace:

```text
|W_f/weight_f - W_g/weight_g|
<= (D+1) * (c_f_max/weight_f + c_g_max/weight_g).
```

The units are supplied cost. This document does not claim the bound for
intervals containing cancellation because a non-retroactive virtual charge is
not completed work.

No-starvation is claimed only under the preconditions in
[Starvation and progress](THEORY.md#starvation-and-progress): a bounded
registry and queue, positive weights fixed for each registration, a positive
lower bound on normalized increment, the specified FIFO tie rule, finite
completion of every dispatched job, and continuing completion and dispatch
calls. In this specification, `maxFlows` and `maxLiveJobs` provide finiteness,
and the input ranges give `cost/weight >= 1/Long.MAX_VALUE`. A close and new
identity within a busy period are allowed only after debt is repaid, when
resetting identity does not reduce the next start tag. Without external
progress, the scheduler cannot guarantee dispatch.

Required adversarial trace: an accepted head victim remains queued with fixed
`S_v`; after every completion, a competing registered flow temporarily becomes
inactive and is enqueued again before the next dispatch. Its `lastFinish` MUST
NOT be reset while greater than `V`, so the start tags of successive requests
increase by at least its positive normalized increment. Once
`lastFinish <= V`, a new identity still starts at `V` and receives no smaller
key. With a bounded registry and FIFO ties, only finitely many requests can have
a key smaller than the victim's key; the victim must be dispatched. An
implementation that removes an inactive flow while `lastFinish > V` fails this
must-pass trace and does not conform to the specification.

Work conservation means every call to `dispatchUpTo(k>0)` fills
`min(k, freeSlots, queuedJobs)` issue slots. It is not a promise of an automatic
callback or a guarantee of physical-resource saturation when `D` is wrong,
calls are absent, or an executor fails.

## 11. Resource retention and bounds

The scheduler stores `O(liveJobs + registeredFlows)` records. With
`liveJobs <= maxLiveJobs` and `registeredFlows <= maxFlows`, cardinality is
bounded; no structure grows with the number of terminal jobs or past
registrations.

- A successful cancel removes the payload, job ID, and queued record at its LP.
- Dispatch removes the internal payload reference at its LP; the caller retains
  the payload only through the returned result.
- Completion removes the job ID and running record at its LP.
- Deactivation preserves only bounded registration state; successful
  `closeFlow` removes the flow ID and state.
- Terminal handles, IDs, and results are not cached.
- Handles contain only an inert owner token and sequence and do not retain the
  scheduler or caller-domain objects.
- The only scheduler-wide lifetime values are four fixed-width counters and two
  fixed-width sequences, each limited to `Long.MAX_VALUE`.
- Each registration stores three exact cost totals. The never-reused job
  sequence bounds each sum by `Long.MAX_VALUE * Long.MAX_VALUE` (at most 126
  bits); successful close removes the totals with registration state.
- Exact tag components are limited to 4096 bits and exact rebase; the numeric
  limit causes explicit enqueue rejection rather than unbounded growth. Rebase
  requires `O(queuedJobs + registeredFlows)` bounded temporary state.

A `JobHandle`, `FlowHandle`, dispatch result, or snapshot retained by a caller
is outside the library's internal retention. Because handles are inert, they do
not transitively retain the scheduler.

## 12. Deviations and engineering resolutions relative to Jin04

| Topic | Jin04 | Project decision and consequence |
|---|---|---|
| Physical `N` | `D` is the outstanding issue depth of a black-box server | Direct `N` resources require `D=N`; other mappings are an external admission model |
| API and capacity | No Java API or caller permits | `dispatchUpTo(k)` is an irreversible atomic bounded-batch request; an external pump runs after enqueue, completion, and capacity signals, and the core invokes no callback |
| Tie | Ties are arbitrary | Total key `(S, admission sequence)` |
| Busy-period boundary | Plain §3.2 gives no complete rule | At global idle, `V` and `lastFinish` of every registration reset to zero while registrations remain |
| Cancellation | Absent | Queued cancellation only; tags are immutable, virtual charge remains until global idle, and the fairness theorem is not claimed for cancelled intervals |
| Flow identity | A flow is assumed to be a stable algorithmic entity; no API lifecycle exists | Bounded registration, persistent dormant `lastFinish`, and close only while inactive with `lastFinish <= V` |
| Job identity and duplicates | Absent | Inert never-reused capability handles, live-only JobId uniqueness, terminal `NOT_LIVE`, and no tombstones |
| Concurrency | Absent | Linearizable atomic operations and the exact LP table in §8 |
| Batch dispatch | Filling depth is described without API atomicity | One call selects a sequential SFQ(D) batch but linearizes as a whole |
| Completion order | Black-box server | Any running handle may complete; the scheduler imposes no completion order |
| Weight changes | Undefined | Weight is fixed for the registration lifetime; change requires safe close and a new registration |
| Numbers | Mathematical unbounded tags | Exact canonical rationals, fail-closed 4096-bit persistent and 8193-bit transient budgets, and transactional all-registration rebase |
| Retention | Not considered | Payload release, no terminal metadata, and `maxLiveJobs` and `maxFlows` bounds |
| Introspection | Not considered | Exact atomic aggregate and per-registration lifecycle snapshots without tags, identifiers, or a clock |
| Executor rejection | Not considered | Dispatch is irrevocable; the caller must complete, and requeue is a new enqueue |

Min-SFQ(D), FSFQ(D), FlashFQ, MSFQ, MSF²Q, and MQFQ are not added. In
particular, the library has no adjusted tags, GPS eligibility, anticipation,
throttling threshold, or multi-queue relaxed order.

## 13. Model-testing obligations

The reference oracle uses unbounded exact rationals and does not reject a
syntactically valid enqueue because of the bit budget. The production
implementation MUST match the oracle for every accepted prefix up to an
expected bounded rejection. On `NUMERIC_LIMIT`, the comparison harness MUST
confirm against the unbounded candidate that the formal persistent or transient
budget is genuinely violated after the one permitted transactional rebase; the
production state remains unchanged, and oracle state is rolled back so the
shared trace can continue.

When no such expected rejection occurs, the reference model and production
implementation MUST agree on:

- register and close outcomes; the harness creates a logical bijection
  `oracle FlowHandle <-> SUT FlowHandle` for each corresponding successful
  register event without reading token or sequence;
- enqueue outcomes; an analogous logical JobHandle bijection is created for
  each corresponding `ACCEPTED` event;
- ordered dispatch lists compared field by field under §7.5 through these
  logical handle mappings;
- cancel and completion results;
- aggregate and per-flow snapshots;
- rejection without state mutation;
- busy-period reset, persistent dormant histories, snapshot counts, and exact
  per-flow lifecycle costs.

For a concurrency history, the results must permit at least one sequence under
§8. Required model properties are:

1. formulas and exact tag comparison;
2. monotonic dispatch start tags within a busy period;
3. total deterministic tie-breaking;
4. `running <= D` and exact slot accounting;
5. at-most-once dispatch, completion, and cancel success;
6. membership-sensitive cancel and batch race: a cancel winner is never
   dispatched, a selected batch job is not cancelled, and an unselected queued
   job may be cancelled after an earlier batch;
7. registered, inactive, and active transitions, immutable registration weight,
   and safe close;
8. JobId reuse without ABA through the handle;
9. FlowId reuse without ABA through the inert FlowHandle;
10. exact all-registration rebase equivalence, transient and persistent limits,
    and transactional numeric rejection against the unbounded oracle;
11. the adversarial dormant-flow starvation trace from §10;
12. concurrent `maxFlows` capacity and the register, close, enqueue,
    global-idle, and rebase races from §8.1;
13. bounded record cardinality and absence of terminal or payload retention;
14. late `cancel/NOT_LIVE` permits every terminal, stale, and foreign cause and
    is not used alone as proof of the dispatch winner;
15. the §4.2 conservation equations after every event, with mathematical sums
    and no test-side `long` overflow;
16. the exact handle equality and hash-code contract, distinct runtime types,
    no `Comparable` or `Serializable`, and no public token or sequence
    accessors;
17. identity equality for immutable `Dispatch`, an unmodifiable detached result
    list, and field-by-field differential comparison with payload identity.

Every future optimization must preserve this observable model. Changing any
decision in §12 requires changing this specification, the claim scope, and the
model tests first.
