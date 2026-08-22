#!/usr/bin/env bash

set -euo pipefail

declare -a EXPECTED_ARTIFACTS=()
readonly FORBIDDEN_ENV=(
  JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS MAVEN_OPTS MAVEN_ARGS CLASSPATH
)

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

for variable_name in "${FORBIDDEN_ENV[@]}"; do
  if [[ -n "${!variable_name:-}" ]]; then
    fail "${variable_name} must be empty for reproducibility verification"
  fi
done

readonly REPOSITORY_ROOT="$(git rev-parse --show-toplevel)"
readonly SOURCE_COMMIT="$(git -C "${REPOSITORY_ROOT}" rev-parse HEAD)"
if [[ -n "$(git -C "${REPOSITORY_ROOT}" status --porcelain=v1 --untracked-files=all)" ]]; then
  fail "reproducibility verification requires a clean checkout"
fi

readonly TEMPORARY_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/sfqd-reproducible.XXXXXX")"
readonly FIRST_WORKTREE="${TEMPORARY_ROOT}/first"
readonly SECOND_WORKTREE="${TEMPORARY_ROOT}/second"

cleanup() {
  if [[ -e "${FIRST_WORKTREE}/.git" ]]; then
    git -C "${REPOSITORY_ROOT}" worktree remove --force "${FIRST_WORKTREE}" >/dev/null 2>&1 || true
  fi
  if [[ -e "${SECOND_WORKTREE}/.git" ]]; then
    git -C "${REPOSITORY_ROOT}" worktree remove --force "${SECOND_WORKTREE}" >/dev/null 2>&1 || true
  fi
  rmdir "${TEMPORARY_ROOT}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

sha256_file() {
  local artifact_path=$1
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "${artifact_path}" | awk '{print $1}'
  else
    shasum -a 256 "${artifact_path}" | awk '{print $1}'
  fi
}

discover_expected_artifacts() {
  local worktree_path=$1
  local discovery
  local role
  local artifact_path
  local discovered_count=0

  discovery=$("${worktree_path}/tools/verify-core-artifacts.sh" discover \
    "${worktree_path}/sfqd-core/target" "${JAVA_HOME:+${JAVA_HOME}/bin/}jar") \
    || fail "cannot discover core artifacts by content"
  while IFS=$'\t' read -r role artifact_path; do
    case "${role}" in
      binary|sources|javadoc) ;;
      *) fail "unexpected discovered core artifact role: ${role}" ;;
    esac
    [[ "${artifact_path}" == "${worktree_path}"/* ]] \
      || fail "discovered core artifact escapes the build worktree"
    EXPECTED_ARTIFACTS+=("${artifact_path#"${worktree_path}"/}")
    ((discovered_count += 1))
  done <<<"${discovery}"
  [[ ${discovered_count} -eq 3 ]] || fail "expected three content-identified core artifacts"
  EXPECTED_ARTIFACTS+=(
    "sfqd-benchmarks/target/sfqd-benchmarks.jar"
    "sfqd-jcstress/target/sfqd-jcstress.jar"
  )
}

build_worktree() {
  local worktree_path=$1
  git -C "${REPOSITORY_ROOT}" worktree add --detach "${worktree_path}" "${SOURCE_COMMIT}" >/dev/null
  (
    cd "${worktree_path}"
    LC_ALL=C LANG=C TZ=UTC ./mvnw -B -ntp -Pbenchmarks,jcstress -DskipTests clean package
  )
}

build_worktree "${FIRST_WORKTREE}"
sleep 2
build_worktree "${SECOND_WORKTREE}"
discover_expected_artifacts "${FIRST_WORKTREE}"

for relative_path in "${EXPECTED_ARTIFACTS[@]}"; do
  first_artifact="${FIRST_WORKTREE}/${relative_path}"
  second_artifact="${SECOND_WORKTREE}/${relative_path}"
  [[ -f "${first_artifact}" ]] || fail "first build is missing ${relative_path}"
  [[ -f "${second_artifact}" ]] || fail "second build is missing ${relative_path}"
  first_digest="$(sha256_file "${first_artifact}")"
  second_digest="$(sha256_file "${second_artifact}")"
  if [[ "${first_digest}" != "${second_digest}" ]]; then
    fail "SHA-256 mismatch for ${relative_path}: ${first_digest} != ${second_digest}"
  fi
  echo "REPRODUCIBLE ${first_digest}  ${relative_path}"
done

echo "REPRODUCIBLE_BUILD PASS commit=${SOURCE_COMMIT} artifacts=${#EXPECTED_ARTIFACTS[@]}"
