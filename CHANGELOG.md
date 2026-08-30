# Changelog

All notable changes to this project are documented in this file.

## 1.1.0 - 2026-08-30

Adds opt-in virtual-cost refunds for queued cancellation while preserving the
original charged-cost behavior as the default.

### Added

- `CancellationAccounting.REFUND_CANCELLED_COST` for callers that need a
  cancelled queued job's virtual cost returned to later queued work of the
  same flow;
- reference, differential, numeric-boundary, lifecycle, deterministic
  concurrency, and jcstress coverage for refundable cancellation;
- policy-aware JMH workloads for cancellation latency, cancel-and-replace
  cycles, terminal idle reset, and first-busy-period cycles.

### Changed

- refund-mode admission now reserves exact-rational capacity for every future
  queued cancellation and rejects an unsafe enqueue atomically with
  `NUMERIC_LIMIT`;
- refund-mode queued cancellation recomputes only the later queued suffix of
  the same flow and never revises an earlier dispatch decision;
- benchmark fixture verification covers both cancellation policies.

### Compatibility

- `CHARGE_RESERVED_COST` remains the default and retains its previous
  admission and cancellation behavior;
- existing public signatures are unchanged; the public enum gains one
  constant, so exhaustive client switches without a default may need updating;
- completed-work fairness guarantees remain intentionally unspecified for
  traces containing cancellation under either policy.

## 1.0.0 - 2026-08-23

Initial public release of the generic, thread-safe SFQ(D) scheduler.

### Included

- exact rational scheduling tags with bounded, fail-closed numeric handling;
- weighted and cost-aware dispatch with configurable issue depth;
- opaque flow and job handles, cancellation, completion, flow closure, and
  atomic snapshots;
- Java 17 runtime compatibility and no third-party runtime dependencies;
- source and JavaDoc artifacts for Maven Central;
- unit, property, differential, concurrency, retention, static-analysis,
  reproducibility, and publication-topology verification;
- compiled executor and bounded resource-pool integration examples;
- separate jcstress and JMH harnesses.

### Boundaries

- the library schedules work but does not execute it or own external resource
  capacity;
- dispatch fairness begins after admission and does not provide admission
  isolation;
- queued cancellation uses reserved-cost accounting as documented in the
  README and formal specification;
- the repository contains a benchmark protocol, but no published production
  throughput or latency result.
