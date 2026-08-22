#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

archive_contains() {
  local archive_path=$1
  local expected_entry=$2
  local jar_command=$3
  "${jar_command}" tf "${archive_path}" | grep -Fqx "${expected_entry}"
}

classify_artifacts() {
  local target_directory=$1
  local jar_command=$2
  local -a candidates
  local candidate
  local matches
  local role

  CORE_BINARY_ARTIFACT=
  CORE_SOURCES_ARTIFACT=
  CORE_JAVADOC_ARTIFACT=

  shopt -s nullglob
  candidates=("${target_directory}"/*.jar)
  shopt -u nullglob
  [[ ${#candidates[@]} -eq 3 ]] \
    || fail "expected exactly three core JAR artifacts, found ${#candidates[@]}"

  for candidate in "${candidates[@]}"; do
    matches=0
    role=
    if archive_contains "${candidate}" "io/github/pzhin/sfqd/SfqdScheduler.class" "${jar_command}"; then
      matches=$((matches + 1))
      role=binary
    fi
    if archive_contains "${candidate}" "io/github/pzhin/sfqd/SfqdScheduler.java" "${jar_command}"; then
      matches=$((matches + 1))
      role=sources
    fi
    if archive_contains "${candidate}" "io/github/pzhin/sfqd/SfqdScheduler.html" "${jar_command}" \
        && archive_contains "${candidate}" "element-list" "${jar_command}"; then
      matches=$((matches + 1))
      role=javadoc
    fi
    [[ ${matches} -eq 1 ]] \
      || fail "core artifact must match exactly one content identity: ${candidate}"
    case "${role}" in
      binary)
        [[ -z "${CORE_BINARY_ARTIFACT}" ]] || fail "duplicate core binary artifact"
        CORE_BINARY_ARTIFACT=${candidate}
        ;;
      sources)
        [[ -z "${CORE_SOURCES_ARTIFACT}" ]] || fail "duplicate core sources artifact"
        CORE_SOURCES_ARTIFACT=${candidate}
        ;;
      javadoc)
        [[ -z "${CORE_JAVADOC_ARTIFACT}" ]] || fail "duplicate core JavaDoc artifact"
        CORE_JAVADOC_ARTIFACT=${candidate}
        ;;
      *)
        fail "unknown core artifact identity: ${candidate}"
        ;;
    esac
  done

  [[ -n "${CORE_BINARY_ARTIFACT}" ]] || fail "core binary artifact is missing"
  [[ -n "${CORE_SOURCES_ARTIFACT}" ]] || fail "core sources artifact is missing"
  [[ -n "${CORE_JAVADOC_ARTIFACT}" ]] || fail "core JavaDoc artifact is missing"
}

write_expected_public_types() {
  local destination=$1
  printf '%s\n' \
    io.github.pzhin.sfqd.CancelResult \
    io.github.pzhin.sfqd.CancellationAccounting \
    io.github.pzhin.sfqd.CloseFlowResult \
    io.github.pzhin.sfqd.CompletionResult \
    io.github.pzhin.sfqd.Dispatch \
    io.github.pzhin.sfqd.EnqueueResult \
    'io.github.pzhin.sfqd.EnqueueResult$Accepted' \
    'io.github.pzhin.sfqd.EnqueueResult$Rejected' \
    io.github.pzhin.sfqd.FlowHandle \
    io.github.pzhin.sfqd.JobHandle \
    io.github.pzhin.sfqd.RegisterFlowResult \
    'io.github.pzhin.sfqd.RegisterFlowResult$Registered' \
    'io.github.pzhin.sfqd.RegisterFlowResult$Rejected' \
    io.github.pzhin.sfqd.SchedulerConfig \
    io.github.pzhin.sfqd.SchedulerSnapshot \
    io.github.pzhin.sfqd.SfqdScheduler \
    | LC_ALL=C sort >"${destination}"
}

verify_binary_entries() {
  local target_directory=$1
  local jar_command=$2
  local temporary_directory=$3

  find "${target_directory}/classes" -type f -name '*.class' \
    | sed "s#^${target_directory}/classes/##" \
    | LC_ALL=C sort >"${temporary_directory}/expected-binary-entries"
  "${jar_command}" tf "${CORE_BINARY_ARTIFACT}" \
    | grep -E '\.class$' \
    | LC_ALL=C sort >"${temporary_directory}/actual-binary-entries"
  if ! diff -u "${temporary_directory}/expected-binary-entries" \
      "${temporary_directory}/actual-binary-entries"; then
    fail "binary JAR classes differ from the compiled main output"
  fi
}

verify_source_entries() {
  local project_directory=$1
  local jar_command=$2
  local temporary_directory=$3

  find "${project_directory}/src/main/java" -type f -name '*.java' \
    | sed "s#^${project_directory}/src/main/java/##" \
    | LC_ALL=C sort >"${temporary_directory}/expected-source-entries"
  "${jar_command}" tf "${CORE_SOURCES_ARTIFACT}" \
    | grep -E '\.java$' \
    | LC_ALL=C sort >"${temporary_directory}/actual-source-entries"
  if ! diff -u "${temporary_directory}/expected-source-entries" \
      "${temporary_directory}/actual-source-entries"; then
    fail "sources JAR differs from the main source tree"
  fi
}

verify_archive_licenses() {
  local project_directory=$1
  local jar_command=$2
  local temporary_directory=$3
  local archive_path
  local archive_role
  local extraction_directory
  local license_path

  license_path=${project_directory}/../LICENSE
  [[ -f "${license_path}" ]] || fail "project LICENSE is missing"
  for archive_role in binary sources javadoc; do
    case "${archive_role}" in
      binary) archive_path=${CORE_BINARY_ARTIFACT} ;;
      sources) archive_path=${CORE_SOURCES_ARTIFACT} ;;
      javadoc) archive_path=${CORE_JAVADOC_ARTIFACT} ;;
    esac
    archive_path=$(cd "$(dirname "${archive_path}")" && pwd -P)/$(basename "${archive_path}")
    extraction_directory=${temporary_directory}/${archive_role}-license
    mkdir -p "${extraction_directory}"
    (
      cd "${extraction_directory}"
      "${jar_command}" xf "${archive_path}" META-INF/LICENSE
    )
    [[ -f "${extraction_directory}/META-INF/LICENSE" ]] \
      || fail "${archive_role} JAR does not contain META-INF/LICENSE"
    if ! cmp -s "${license_path}" \
        "${extraction_directory}/META-INF/LICENSE"; then
      fail "${archive_role} JAR license differs from the project LICENSE"
    fi
  done
}

verify_public_binary_types() {
  local javap_command=$1
  local temporary_directory=$2
  local entry
  local binary_name

  write_expected_public_types "${temporary_directory}/expected-public-types"
  : >"${temporary_directory}/actual-public-types"
  while IFS= read -r entry; do
    [[ "${entry}" == module-info.class || "${entry}" == */package-info.class ]] && continue
    binary_name=${entry%.class}
    binary_name=${binary_name//\//.}
    if "${javap_command}" -classpath "${CORE_BINARY_ARTIFACT}" -public "${binary_name}" \
        | grep -Eq '^public '; then
      printf '%s\n' "${binary_name}" >>"${temporary_directory}/actual-public-types"
    fi
  done <"${temporary_directory}/actual-binary-entries"
  LC_ALL=C sort -o "${temporary_directory}/actual-public-types" \
    "${temporary_directory}/actual-public-types"
  if ! diff -u "${temporary_directory}/expected-public-types" \
      "${temporary_directory}/actual-public-types"; then
    fail "binary JAR public type surface differs from the specified API"
  fi
}

verify_javadoc_types() {
  local jar_command=$1
  local temporary_directory=$2
  local binary_name
  local simple_name

  : >"${temporary_directory}/expected-javadoc-types"
  while IFS= read -r binary_name; do
    simple_name=${binary_name#io.github.pzhin.sfqd.}
    simple_name=$(printf '%s' "${simple_name}" | tr '$' '.')
    printf 'io/github/pzhin/sfqd/%s.html\n' "${simple_name}" \
      >>"${temporary_directory}/expected-javadoc-types"
  done <"${temporary_directory}/expected-public-types"
  LC_ALL=C sort -o "${temporary_directory}/expected-javadoc-types" \
    "${temporary_directory}/expected-javadoc-types"
  "${jar_command}" tf "${CORE_JAVADOC_ARTIFACT}" \
    | grep -E '^io/github/pzhin/sfqd/[^/]+\.html$' \
    | grep -Ev '^io/github/pzhin/sfqd/package-(summary|tree|use)\.html$' \
    | LC_ALL=C sort >"${temporary_directory}/actual-javadoc-types"
  if ! diff -u "${temporary_directory}/expected-javadoc-types" \
      "${temporary_directory}/actual-javadoc-types"; then
    fail "JavaDoc JAR type pages differ from the specified public API"
  fi
}

verify_no_tooling_package() {
  local jar_command=$1
  local archive_path
  for archive_path in \
      "${CORE_BINARY_ARTIFACT}" "${CORE_SOURCES_ARTIFACT}" "${CORE_JAVADOC_ARTIFACT}"; do
    if "${jar_command}" tf "${archive_path}" | grep -Eq '^io/github/pzhin/sfqd/tooling(/|$)'; then
      fail "obsolete tooling package is present in ${archive_path}"
    fi
  done
}

verify_artifacts() {
  local project_directory=$1
  local target_directory=$2
  local jar_command=$3
  local javap_command=$4
  local temporary_directory

  [[ -d "${project_directory}/src/main/java" ]] || fail "main source directory is missing"
  [[ -d "${target_directory}/classes" ]] || fail "compiled main output is missing"
  classify_artifacts "${target_directory}" "${jar_command}"
  temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/sfqd-core-artifacts.XXXXXX")
  trap "rm -rf '${temporary_directory}'" EXIT
  verify_binary_entries "${target_directory}" "${jar_command}" "${temporary_directory}"
  verify_source_entries "${project_directory}" "${jar_command}" "${temporary_directory}"
  verify_archive_licenses "${project_directory}" "${jar_command}" "${temporary_directory}"
  verify_public_binary_types "${javap_command}" "${temporary_directory}"
  verify_javadoc_types "${jar_command}" "${temporary_directory}"
  verify_no_tooling_package "${jar_command}"
  rm -rf "${temporary_directory}"
  trap - EXIT
  echo "CORE_ARTIFACTS PASS binary=$(basename "${CORE_BINARY_ARTIFACT}")" \
    "sources=$(basename "${CORE_SOURCES_ARTIFACT}")" \
    "javadoc=$(basename "${CORE_JAVADOC_ARTIFACT}")"
}

self_test_discovery() {
  local jar_command=$1
  local temporary_directory
  local target_directory
  local payload_directory

  temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/sfqd-artifact-discovery.XXXXXX")
  trap "rm -rf '${temporary_directory}'" EXIT
  target_directory=${temporary_directory}/target
  mkdir -p "${target_directory}"

  payload_directory=${temporary_directory}/binary
  mkdir -p "${payload_directory}/io/github/pzhin/sfqd"
  : >"${payload_directory}/io/github/pzhin/sfqd/SfqdScheduler.class"
  "${jar_command}" cf "${target_directory}/sfqd-core-9.8.7-sources.jar" \
    -C "${payload_directory}" .

  payload_directory=${temporary_directory}/sources
  mkdir -p "${payload_directory}/io/github/pzhin/sfqd"
  : >"${payload_directory}/io/github/pzhin/sfqd/SfqdScheduler.java"
  "${jar_command}" cf "${target_directory}/sfqd-core-9.8.7-sources-sources.jar" \
    -C "${payload_directory}" .

  payload_directory=${temporary_directory}/javadoc
  mkdir -p "${payload_directory}/io/github/pzhin/sfqd"
  : >"${payload_directory}/io/github/pzhin/sfqd/SfqdScheduler.html"
  : >"${payload_directory}/element-list"
  "${jar_command}" cf "${target_directory}/sfqd-core-9.8.7-sources-javadoc.jar" \
    -C "${payload_directory}" .

  classify_artifacts "${target_directory}" "${jar_command}"
  [[ $(basename "${CORE_BINARY_ARTIFACT}") == sfqd-core-9.8.7-sources.jar ]] \
    || fail "content discovery confused a binary whose version ends in -sources"
  [[ $(basename "${CORE_SOURCES_ARTIFACT}") == sfqd-core-9.8.7-sources-sources.jar ]] \
    || fail "content discovery did not identify the sources classifier"
  [[ $(basename "${CORE_JAVADOC_ARTIFACT}") == sfqd-core-9.8.7-sources-javadoc.jar ]] \
    || fail "content discovery did not identify the JavaDoc classifier"
  rm -rf "${temporary_directory}"
  trap - EXIT
  echo "CORE_ARTIFACT_DISCOVERY_SELF_TEST PASS version=9.8.7-sources"
}

readonly MODE=${1:-}
readonly DEFAULT_JAR_COMMAND=${JAVA_HOME:+${JAVA_HOME}/bin/}jar
readonly DEFAULT_JAVAP_COMMAND=${JAVA_HOME:+${JAVA_HOME}/bin/}javap

case "${MODE}" in
  discover)
    [[ $# -ge 2 && $# -le 3 ]] || fail "usage: $0 discover TARGET_DIRECTORY [JAR_COMMAND]"
    classify_artifacts "$2" "${3:-${DEFAULT_JAR_COMMAND}}"
    printf 'binary\t%s\n' "${CORE_BINARY_ARTIFACT}"
    printf 'sources\t%s\n' "${CORE_SOURCES_ARTIFACT}"
    printf 'javadoc\t%s\n' "${CORE_JAVADOC_ARTIFACT}"
    ;;
  self-test)
    [[ $# -le 2 ]] || fail "usage: $0 self-test [JAR_COMMAND]"
    self_test_discovery "${2:-${DEFAULT_JAR_COMMAND}}"
    ;;
  verify)
    [[ $# -ge 3 && $# -le 5 ]] \
      || fail "usage: $0 verify PROJECT_DIRECTORY TARGET_DIRECTORY [JAR_COMMAND] [JAVAP_COMMAND]"
    self_test_discovery "${4:-${DEFAULT_JAR_COMMAND}}"
    verify_artifacts "$2" "$3" "${4:-${DEFAULT_JAR_COMMAND}}" \
      "${5:-${DEFAULT_JAVAP_COMMAND}}"
    ;;
  *)
    fail "mode must be discover, self-test, or verify"
    ;;
esac
