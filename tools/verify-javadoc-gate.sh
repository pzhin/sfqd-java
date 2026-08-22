#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/sfqd-javadoc-gate.XXXXXX")
WORKTREE="$TEMP_ROOT/repo"
LOG="$TEMP_ROOT/build.log"
BYPASS_LOG="$TEMP_ROOT/bypass.log"

cleanup() {
  git -C "$ROOT" worktree remove --force "$WORKTREE" >/dev/null 2>&1 || true
  rm -rf "$TEMP_ROOT"
}
trap cleanup EXIT

git -C "$ROOT" diff --quiet
git -C "$ROOT" diff --cached --quiet
git -C "$ROOT" worktree add --detach "$WORKTREE" HEAD >/dev/null

mkdir -p "$WORKTREE/sfqd-core/src/main/java/io/github/pzhin/sfqd"
cat >"$WORKTREE/sfqd-core/src/main/java/io/github/pzhin/sfqd/UndocumentedPublicApi.java" <<'EOF'
package io.github.pzhin.sfqd;

public final class UndocumentedPublicApi {
    public UndocumentedPublicApi() {
    }

    public int value() {
        return 1;
    }
}
EOF

run_mutated_build() {
  local log=$1
  shift
  if (
    cd "$WORKTREE"
    JAVA_HOME="${JAVA_HOME:?JAVA_HOME must point to JDK 25}" \
      ./mvnw --batch-mode --no-transfer-progress \
      -pl sfqd-core -am -DskipTests "$@" package
  ) >"$log" 2>&1; then
    return 0
  fi
  return 1
}

is_javadoc_failure() {
  local log=$1
  grep -Fq \
    'maven-javadoc-plugin:3.12.0:jar (attach-javadocs)' "$log" &&
    grep -Fq 'Project contains Javadoc Warnings' "$log" &&
    grep -Fq 'UndocumentedPublicApi.java:3: warning: no comment' "$log" &&
    grep -Fq 'UndocumentedPublicApi.java:4: warning: no comment' "$log" &&
    grep -Fq 'UndocumentedPublicApi.java:7: warning: no comment' "$log"
}

if run_mutated_build "$LOG"; then
  echo "ERROR: undocumented public API passed the JavaDoc gate" >&2
  exit 1
fi

if ! is_javadoc_failure "$LOG"; then
  echo "ERROR: mutated build did not fail at the JavaDoc goal" >&2
  sed -n '1,240p' "$LOG" >&2
  exit 1
fi

if run_mutated_build "$BYPASS_LOG" clean \
    -Dmaven.javadoc.failOnWarnings=false; then
  echo "ERROR: disabled JavaDoc gate escaped every later guard" >&2
  exit 1
fi

if is_javadoc_failure "$BYPASS_LOG"; then
  echo "ERROR: unrelated package failure matched the JavaDoc classifier" >&2
  exit 1
fi

if ! grep -Fq \
    'exec-maven-plugin:3.6.3:exec (verify-published-core-artifacts)' \
    "$BYPASS_LOG" ||
    ! grep -Fq \
      'ERROR: binary JAR public type surface differs from the specified API' \
      "$BYPASS_LOG"; then
  echo "ERROR: disabled-gate mutation failed for an unexpected reason" >&2
  sed -n '1,240p' "$BYPASS_LOG" >&2
  exit 1
fi

echo "JAVADOC_NEGATIVE_GATE PASS"
