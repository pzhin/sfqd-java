# Build, verification, and publication tooling

This document describes the commands a contributor or release engineer needs.
Algorithm behavior belongs in [FORMAL_SPEC.md](FORMAL_SPEC.md); normal use is
documented in the [README](../README.md).

## Required environment

- a POSIX-like environment with Bash, Git, `awk`, `sort`, `diff`, and `cmp`;
- JDK 25, including `java`, `javac`, `jar`, and `javap`;
- `JAVA_HOME` pointing to that JDK for repository verifier scripts;
- the checked-in Maven Wrapper, pinned to Maven 3.9.16;
- Python 3 for the isolated publication-consumer verifier;
- either GNU `sha256sum` or macOS/Perl `shasum` for reproducibility checks.

Maven Enforcer rejects a different Java feature release or an older Maven.
Production code is compiled with `--release 25`, `-Xlint:all`, and `-Werror`.

## Modules

| Module | Purpose | Published |
| --- | --- | --- |
| `sfqd-core` | runtime library and ordinary tests | parent and core are deployable |
| `sfqd-coverage` | aggregate JaCoCo report | no |
| `sfqd-jcstress` | JVM concurrency litmus tests | no |
| `sfqd-benchmarks` | JMH workloads and fixture smoke | no |

The default reactor builds only core and coverage. The two harness modules are
enabled explicitly so their dependencies cannot leak into the runtime graph.

## Default verification

```shell
./mvnw --batch-mode --no-transfer-progress clean verify
```

This command blocks on:

- Checkstyle source rules;
- Java compiler warnings;
- JUnit and jqwik tests, including `*Properties` classes;
- at least 80% line coverage for all `sfqd-core` production classes;
- SpotBugs at maximum effort with zero allowed findings;
- JavaDoc errors or warnings;
- exact binary/source/JavaDoc artifact contents and public API surface.

`tools/verify-javadoc-gate.sh` adds an undocumented public class, constructor,
and method in a detached worktree. The build must fail at the JavaDoc goal. A
second mutation disables that goal and proves a later artifact-surface failure
cannot be mistaken for JavaDoc evidence.

## Concurrency verification

```shell
./mvnw --batch-mode --no-transfer-progress -Pjcstress clean verify
```

The profile builds the generated jcstress metadata, runs every repository
litmus in bounded quick mode, and applies Checkstyle and SpotBugs to handwritten
harness code. Platform capability probes reported as unsupported by jcstress
are not test failures; forbidden test outcomes are.

## Benchmark harness

```shell
./mvnw --batch-mode --no-transfer-progress -Pbenchmarks clean verify
java -jar sfqd-benchmarks/target/sfqd-benchmarks.jar -l
```

Verification compiles the generated JMH metadata, runs a bounded 60-case
idle-reset fixture smoke, and exercises three representative performance-scale
fixtures. The largest smoke fixture is `B=10_000`, `Q=100_000`, and
`depth=1_024`; completing one invariant-checked cycle proves wiring and state
restoration, not throughput or latency. Long measurements are optional,
machine-specific work and are intentionally not committed to the release
branch.

## Reproducible archives

```shell
tools/verify-reproducible-build.sh
```

The verifier creates two detached worktrees at the same clean commit, performs
wall-time-separated builds, and compares SHA-256 digests for the core binary,
source JAR, JavaDoc JAR, shaded JMH JAR, and shaded jcstress JAR. Temporary
worktrees are removed on exit.

Project archives use a pinned UTC output timestamp. This proves repeatability
for the same commit, JDK, Maven, and filesystem environment; it is not a claim
that arbitrary toolchains produce identical bytes.

## Publication topology

```shell
tools/verify-publication-topology.sh
```

The script deploys the parent and core to a temporary local file repository,
checks that coverage and harness artifacts were skipped, and compiles a fresh
consumer with a separate Maven local repository. The temporary repository is
deleted afterward.

There is no external `distributionManagement` target. An ordinary deploy
without an explicit alternate repository therefore fails instead of publishing
somewhere unexpected. The source branch records the Maven coordinates, Java
package, Apache-2.0 license, project URL, SCM, and developer metadata. A public
release still requires a non-snapshot release decision, destination
configuration, credentials, signing policy, tag, and release notes.

## CI jobs

GitHub Actions runs five independent jobs:

1. default quality gates and the negative JavaDoc mutation;
2. all jcstress tests in quick mode;
3. two-build archive reproducibility;
4. local publication topology plus independent consumer resolution;
5. benchmark packaging, discovery, fixture smoke, and one short JMH iteration.

All actions are pinned by full commit SHA and receive read-only repository
permissions.

## Pinned tools

| Tool | Version |
| --- | ---: |
| JDK | 25 |
| Maven Wrapper / Maven | 3.3.4 / 3.9.16 |
| Checkstyle | 14.0.0 |
| SpotBugs | 4.10.3 |
| JUnit | 5.13.1 |
| jqwik | 1.9.3 |
| JaCoCo | 0.8.15 |
| JMH | 1.37 |
| jcstress | 0.16 |
