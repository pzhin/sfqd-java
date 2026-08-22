#!/usr/bin/env bash

set -euo pipefail

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

[[ $# -eq 5 ]] \
  || fail "usage: $0 LICENSE TARGET_DIRECTORY FINAL_NAME JAR_COMMAND OUTPUT_TIMESTAMP"

readonly LICENSE_PATH=$1
readonly TARGET_DIRECTORY=$2
readonly FINAL_NAME=$3
readonly JAR_COMMAND=$4
readonly OUTPUT_TIMESTAMP=$5

[[ -f "${LICENSE_PATH}" ]] || fail "project LICENSE is missing"
[[ -d "${TARGET_DIRECTORY}" ]] || fail "target directory is missing"

readonly TEMPORARY_BASE=${TMPDIR:-/tmp}
readonly TEMPORARY_DIRECTORY=$(mktemp -d "${TEMPORARY_BASE%/}/sfqd-license.XXXXXX")

cleanup() {
  case "${TEMPORARY_DIRECTORY}" in
    "${TEMPORARY_BASE%/}"/sfqd-license.*)
      rm -rf -- "${TEMPORARY_DIRECTORY}"
      ;;
    *)
      echo "ERROR: refusing to remove unexpected temporary path ${TEMPORARY_DIRECTORY}" >&2
      ;;
  esac
}
trap cleanup EXIT

mkdir -p "${TEMPORARY_DIRECTORY}/META-INF"
cp "${LICENSE_PATH}" "${TEMPORARY_DIRECTORY}/META-INF/LICENSE"

for classifier in sources javadoc; do
  archive_path=${TARGET_DIRECTORY}/${FINAL_NAME}-${classifier}.jar
  [[ -f "${archive_path}" ]] || fail "attached ${classifier} JAR is missing"
  "${JAR_COMMAND}" --update --file "${archive_path}" \
    --date "${OUTPUT_TIMESTAMP}" \
    -C "${TEMPORARY_DIRECTORY}" META-INF/LICENSE
done

echo "ATTACHED_ARTIFACT_LICENSES PASS sources=present javadoc=present"
