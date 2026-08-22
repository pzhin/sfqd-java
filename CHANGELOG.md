# Changelog

All notable changes to this project are documented in this file.

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
