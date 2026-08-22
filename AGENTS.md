# Repository instructions

## Scope

This file applies to the entire repository. A more specific `AGENTS.md` may
supplement or refine it for its directory. User instructions take precedence
over these general rules.

Keep every change within the requested scope. Do not include unrelated cleanup
or refactoring.

## Find the authoritative context

| Source | Use it for |
| --- | --- |
| [`README.md`](README.md) | Public positioning, the primary user workflow, and the high-level API |
| [`docs/FORMAL_SPEC.md`](docs/FORMAL_SPEC.md) | Normative scheduling, numeric, concurrency, and lifecycle contracts |
| [`docs/THEORY.md`](docs/THEORY.md) | Theoretical basis and the boundaries of claimed properties |
| [`docs/EXECUTOR_INTEGRATION.md`](docs/EXECUTOR_INTEGRATION.md) | Rules and examples for integration with external execution |
| [`docs/TOOLING.md`](docs/TOOLING.md) | Supported environments and verification commands |
| [`pom.xml`](pom.xml) and module POMs | Actual build and module configuration |
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | Current CI jobs and required verification topology |
| [`sfqd-core/src/test/java/io/github/pzhin/sfqd/`](sfqd-core/src/test/java/io/github/pzhin/sfqd/) | Production and reference-model executable evidence |

Treat the formal specification as normative and the POMs and CI workflow as
authoritative for build details.

## Change workflow

1. Read the documents relevant to the affected behavior.
2. Inspect the current implementation and existing tests.
3. Define the normative contract of the change before editing code.
4. For a suspected defect, first build a minimal reproducible trace or
   regression test when practical.
5. Make the smallest coherent change.
6. Run targeted tests for the changed behavior.
7. Check every related document for impact.
8. In the same change, update documentation when the contract, public
   behavior, API, limitations, commands, or supported environment changes.
9. Run the complete applicable verification path before finishing.
10. Report only checks that were actually run.

## Keep documentation, tests, and implementation consistent

Every change requires a review of related documentation. Do not merely note a
discrepancy and leave it unresolved. If the task and normative documents make
the intended behavior unambiguous, align the code, tests, and documentation in
the same coherent change.

Do not silently change implementation contrary to
[`docs/FORMAL_SPEC.md`](docs/FORMAL_SPEC.md). A normative contract change must
be explicit in the task or separately approved. If it is unclear whether the
contract or the implementation should change, stop and ask a human. Do not
defer a necessary documentation correction without a specific reason.

## Test discipline

- A hypothesis is not a confirmed defect without a reproducible trace, a
  logical proof, or an observed failure.
- For scheduling and numeric changes, run the applicable reference,
  differential, property, and boundary tests already in the repository.
- Include a targeted regression for a defect when one is practical.
- For failed and rejected operations, also verify that no partial mutation
  occurred.
- For concurrency changes, verify linearization behavior and run the existing
  concurrency suites.
- Do not weaken a test merely to make the build green unless there is evidence
  that its previous expectation was wrong.

Use the formal specification and relevant tests as the source of concrete
invariants.

## Build and verification

Use JDK 17 or newer and the checked-in Maven Wrapper. Published classes target
Java 17. On POSIX systems use `./mvnw`; on Windows use `./mvnw.cmd` with the
same Maven arguments.

Start with the narrowest relevant tests. For example, a core test class can be
run with:

```shell
./mvnw --batch-mode --no-transfer-progress -pl sfqd-core \
  -Dtest=SfqdSchedulerTest test
```

Replace the selected class with the test or property class covering the
changed behavior. Targeted tests do not replace the full verification path.

For an ordinary change, the required local verification is:

```shell
./mvnw --batch-mode --no-transfer-progress clean verify
```

Run these additional checks when their area is affected:

```shell
# Concurrency behavior or harness changes
./mvnw --batch-mode --no-transfer-progress -Pjcstress clean verify

# Benchmark code, fixtures, packaging, or discovery
./mvnw --batch-mode --no-transfer-progress -Pbenchmarks clean verify
java -jar sfqd-benchmarks/target/sfqd-benchmarks.jar -l

# The fail-closed public-JavaDoc gate or its build wiring
tools/verify-javadoc-gate.sh

# Reproducible release artifacts
tools/verify-reproducible-build.sh

# Deployment topology or consumer resolution
tools/verify-publication-topology.sh
```

The standalone release verifiers require the POSIX-like tools listed in
[`docs/TOOLING.md`](docs/TOOLING.md). Long JMH measurements are optional,
machine-specific work; follow
[`sfqd-benchmarks/README.md`](sfqd-benchmarks/README.md), retain the raw
evidence, and do not treat smoke runs as performance results.

CI runs the default verification on Linux and Windows with JDK 17, 21, and 25,
plus separate JavaDoc, jcstress, reproducibility, publication, and benchmark
jobs. Keep the required topology in
[`.github/workflows/ci.yml`](.github/workflows/ci.yml) intact.

## Public API and release-sensitive changes

Before changing public types, methods, constructors, modifiers, generic
signatures, or the sealed hierarchy, inspect the documentation and existing
compatibility gates. Public changes require tests and updated user
documentation. Do not introduce an incompatible change as a side effect of an
internal refactoring.

Do not invent release metadata, coordinates, publication destinations, or
supported platforms. Ask a human when a release-sensitive decision is not
unambiguously established by the task and repository.

## Before finishing

- Inspect the worktree and remove or separate unrelated changes.
- Confirm that code, tests, and documentation agree.
- List every changed file.
- Briefly describe the observable behavior change, if any.
- List each executed check and its result.
- List unexecuted applicable checks and the reason.
- Do not claim readiness while a required check is failing.
