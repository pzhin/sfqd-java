#!/usr/bin/env bash

set -euo pipefail

readonly FORBIDDEN_ENV=(
  JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS MAVEN_OPTS MAVEN_ARGS CLASSPATH
)

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

for variable_name in "${FORBIDDEN_ENV[@]}"; do
  if [[ -n "${!variable_name:-}" ]]; then
    fail "${variable_name} must be empty for publication verification"
  fi
done

readonly REPOSITORY_ROOT="$(git rev-parse --show-toplevel)"
if [[ -n "$(git -C "${REPOSITORY_ROOT}" status --porcelain=v1 --untracked-files=all)" ]]; then
  fail "publication verification requires a clean checkout"
fi

python3 - "${REPOSITORY_ROOT}" <<'PY'
import pathlib
import sys
import xml.etree.ElementTree as ET

root = pathlib.Path(sys.argv[1])
namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
parent = ET.parse(root / "pom.xml")
parent_skip = parent.findtext("m:properties/m:maven.deploy.skip", namespaces=namespace)
if parent_skip != "false":
    raise SystemExit(
        f"ERROR: pom.xml must set maven.deploy.skip=false; got {parent_skip!r}"
    )

modules = {
    element.text
    for element in parent.findall("m:modules/m:module", namespace)
    + parent.findall("m:profiles/m:profile/m:modules/m:module", namespace)
}
if "sfqd-core" not in modules:
    raise SystemExit("ERROR: the reactor does not contain sfqd-core")
for module in sorted(modules):
    relative_path = pathlib.Path(module) / "pom.xml"
    document = ET.parse(root / relative_path)
    value = document.findtext("m:properties/m:maven.deploy.skip", namespaces=namespace)
    expected_value = "false" if module == "sfqd-core" else "true"
    if value != expected_value:
        raise SystemExit(
            f"ERROR: {relative_path} must set maven.deploy.skip={expected_value}; got {value!r}"
        )
PY

readonly TEMPORARY_BASE="${TMPDIR:-/tmp}"
readonly TEMPORARY_ROOT="$(mktemp -d "${TEMPORARY_BASE%/}/sfqd-publication.XXXXXX")"
readonly DEPLOY_REPOSITORY="${TEMPORARY_ROOT}/deployed"
readonly CONSUMER_ROOT="${TEMPORARY_ROOT}/consumer"
readonly CONSUMER_LOCAL_REPOSITORY="${TEMPORARY_ROOT}/consumer-repository"
readonly EFFECTIVE_CORE_POM="${TEMPORARY_ROOT}/effective-core-pom.xml"

cleanup() {
  case "${TEMPORARY_ROOT}" in
    "${TEMPORARY_BASE%/}"/sfqd-publication.*)
      rm -rf -- "${TEMPORARY_ROOT}"
      ;;
    *)
      echo "ERROR: refusing to remove unexpected temporary path ${TEMPORARY_ROOT}" >&2
      ;;
  esac
}
trap cleanup EXIT

mkdir -p "${DEPLOY_REPOSITORY}" "${CONSUMER_ROOT}/src/main/java/smoke"
readonly DEPLOY_REPOSITORY_URI="$(
  python3 - "${DEPLOY_REPOSITORY}" <<'PY'
import pathlib
import sys

print(pathlib.Path(sys.argv[1]).resolve().as_uri())
PY
)"

(
  cd "${REPOSITORY_ROOT}"
  LC_ALL=C LANG=C TZ=UTC ./mvnw --batch-mode --no-transfer-progress \
    -pl sfqd-core help:effective-pom -Doutput="${EFFECTIVE_CORE_POM}"
)

(
  cd "${REPOSITORY_ROOT}"
  LC_ALL=C LANG=C TZ=UTC ./mvnw --batch-mode --no-transfer-progress \
    -Pbenchmarks,jcstress \
    -DaltDeploymentRepository="publication-smoke::${DEPLOY_REPOSITORY_URI}" \
    -Dexec.skip=true -Djacoco.skip=true -DskipTests -Dspotbugs.skip=true \
    clean deploy
)

readonly GROUP_DIRECTORY="${DEPLOY_REPOSITORY}/io/github/pzhin"
readonly PARENT_DIRECTORY="${GROUP_DIRECTORY}/sfqd-java-parent/0.1.0-SNAPSHOT"
readonly CORE_DIRECTORY="${GROUP_DIRECTORY}/sfqd-core/0.1.0-SNAPSHOT"

shopt -s nullglob
parent_poms=("${PARENT_DIRECTORY}"/*.pom)
core_poms=("${CORE_DIRECTORY}"/*.pom)
core_jars=("${CORE_DIRECTORY}"/*.jar)
if [[ ${#parent_poms[@]} -ne 1 ]]; then
  fail "temporary repository must contain exactly one deployed parent POM"
fi
if [[ ${#core_poms[@]} -ne 1 ]]; then
  fail "temporary repository must contain exactly one deployed core POM"
fi
if [[ ${#core_jars[@]} -ne 3 ]]; then
  fail "temporary repository must contain core binary, sources, and JavaDoc JARs"
fi

core_binary_jars=()
for core_jar in "${core_jars[@]}"; do
  case "${core_jar}" in
    *-sources.jar | *-javadoc.jar) ;;
    *) core_binary_jars+=("${core_jar}") ;;
  esac
done
if [[ ${#core_binary_jars[@]} -ne 1 ]]; then
  fail "temporary repository must contain exactly one core binary JAR"
fi
if jar tf "${core_binary_jars[0]}" | grep -Eq \
  '^io/github/pzhin/sfqd/(Reference[^/]*\.class|benchmarks/|jcstress/)'; then
  fail "deployed core binary contains reference-model or harness classes"
fi

for forbidden_artifact in sfqd-coverage sfqd-benchmarks sfqd-jcstress; do
  if [[ -e "${GROUP_DIRECTORY}/${forbidden_artifact}" ]]; then
    fail "temporary repository unexpectedly contains ${forbidden_artifact}"
  fi
done

python3 - "${parent_poms[0]}" "${core_poms[0]}" "${EFFECTIVE_CORE_POM}" <<'PY'
import sys
import xml.etree.ElementTree as ET

namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
expected_parent = ("io.github.pzhin", "sfqd-java-parent", "0.1.0-SNAPSHOT")

parent = ET.parse(sys.argv[1]).getroot()
parent_coordinates = tuple(
    parent.findtext(f"m:{name}", namespaces=namespace)
    for name in ("groupId", "artifactId", "version")
)
if parent_coordinates != expected_parent:
    raise SystemExit(
        f"ERROR: deployed parent coordinates are {parent_coordinates!r}, expected {expected_parent!r}"
    )

expected_project_url = "https://github.com/pzhin/sfqd-java"
project_url = parent.findtext("m:url", namespaces=namespace)
if project_url != expected_project_url:
    raise SystemExit(
        f"ERROR: deployed project URL is {project_url!r}, expected {expected_project_url!r}"
    )

license_element = parent.find("m:licenses/m:license", namespace)
if license_element is None:
    raise SystemExit("ERROR: deployed parent POM has no license metadata")
license_metadata = tuple(
    license_element.findtext(f"m:{name}", namespaces=namespace)
    for name in ("name", "url", "distribution")
)
expected_license = (
    "Apache License, Version 2.0",
    "https://www.apache.org/licenses/LICENSE-2.0.txt",
    "repo",
)
if license_metadata != expected_license:
    raise SystemExit(
        f"ERROR: deployed license metadata is {license_metadata!r}, expected {expected_license!r}"
    )

developer_element = parent.find("m:developers/m:developer", namespace)
if developer_element is None:
    raise SystemExit("ERROR: deployed parent POM has no developer metadata")
developer_metadata = tuple(
    developer_element.findtext(f"m:{name}", namespaces=namespace)
    for name in ("id", "name", "email", "url")
)
expected_developer = (
    "pzhin",
    "Pavel Nevezhin",
    "nevezhin.pavel@gmail.com",
    "https://github.com/pzhin",
)
if developer_metadata != expected_developer:
    raise SystemExit(
        f"ERROR: deployed developer metadata is {developer_metadata!r}, "
        f"expected {expected_developer!r}"
    )

scm_element = parent.find("m:scm", namespace)
if scm_element is None:
    raise SystemExit("ERROR: deployed parent POM has no SCM metadata")
scm_metadata = tuple(
    scm_element.findtext(f"m:{name}", namespaces=namespace)
    for name in ("connection", "developerConnection", "url", "tag")
)
expected_scm = (
    "scm:git:https://github.com/pzhin/sfqd-java.git",
    "scm:git:ssh://git@github.com/pzhin/sfqd-java.git",
    "https://github.com/pzhin/sfqd-java",
    "HEAD",
)
if scm_metadata != expected_scm:
    raise SystemExit(
        f"ERROR: deployed SCM metadata is {scm_metadata!r}, expected {expected_scm!r}"
    )

core = ET.parse(sys.argv[2]).getroot()
core_parent = core.find("m:parent", namespace)
if core_parent is None:
    raise SystemExit("ERROR: deployed core POM has no parent")
core_parent_coordinates = tuple(
    core_parent.findtext(f"m:{name}", namespaces=namespace)
    for name in ("groupId", "artifactId", "version")
)
if core_parent_coordinates != expected_parent:
    raise SystemExit(
        "ERROR: deployed core POM references "
        f"{core_parent_coordinates!r}, expected {expected_parent!r}"
    )

effective_core = ET.parse(sys.argv[3]).getroot()
effective_core_coordinates = tuple(
    effective_core.findtext(f"m:{name}", namespaces=namespace)
    for name in ("groupId", "artifactId", "version")
)
expected_core = ("io.github.pzhin", "sfqd-core", "0.1.0-SNAPSHOT")
if effective_core_coordinates != expected_core:
    raise SystemExit(
        "ERROR: effective core coordinates are "
        f"{effective_core_coordinates!r}, expected {expected_core!r}"
    )

effective_project_url = effective_core.findtext("m:url", namespaces=namespace)
if effective_project_url != expected_project_url:
    raise SystemExit(
        "ERROR: effective core project URL is "
        f"{effective_project_url!r}, expected {expected_project_url!r}"
    )

effective_license_element = effective_core.find("m:licenses/m:license", namespace)
if effective_license_element is None:
    raise SystemExit("ERROR: effective core POM has no license metadata")
effective_license = tuple(
    effective_license_element.findtext(f"m:{name}", namespaces=namespace)
    for name in ("name", "url", "distribution")
)
if effective_license != expected_license:
    raise SystemExit(
        "ERROR: effective core license metadata is "
        f"{effective_license!r}, expected {expected_license!r}"
    )

effective_developer_element = effective_core.find("m:developers/m:developer", namespace)
if effective_developer_element is None:
    raise SystemExit("ERROR: effective core POM has no developer metadata")
effective_developer = tuple(
    effective_developer_element.findtext(f"m:{name}", namespaces=namespace)
    for name in ("id", "name", "email", "url")
)
if effective_developer != expected_developer:
    raise SystemExit(
        "ERROR: effective core developer metadata is "
        f"{effective_developer!r}, expected {expected_developer!r}"
    )

effective_scm_element = effective_core.find("m:scm", namespace)
if effective_scm_element is None:
    raise SystemExit("ERROR: effective core POM has no SCM metadata")
effective_scm = tuple(
    effective_scm_element.findtext(f"m:{name}", namespaces=namespace)
    for name in ("connection", "developerConnection", "url")
)
if effective_scm != expected_scm[:3]:
    raise SystemExit(
        "ERROR: effective core SCM metadata is "
        f"{effective_scm!r}, expected {expected_scm[:3]!r}"
    )
PY

cat >"${CONSUMER_ROOT}/pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>publication.smoke</groupId>
  <artifactId>consumer</artifactId>
  <version>1</version>
  <properties>
    <maven.compiler.release>25</maven.compiler.release>
  </properties>
  <repositories>
    <repository>
      <id>publication-smoke</id>
      <url>${DEPLOY_REPOSITORY_URI}</url>
      <releases><enabled>true</enabled></releases>
      <snapshots><enabled>true</enabled><updatePolicy>always</updatePolicy></snapshots>
    </repository>
  </repositories>
  <dependencies>
    <dependency>
      <groupId>io.github.pzhin</groupId>
      <artifactId>sfqd-core</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <version>3.15.0</version>
      </plugin>
    </plugins>
  </build>
</project>
EOF

cat >"${CONSUMER_ROOT}/src/main/java/smoke/Consumer.java" <<'EOF'
package smoke;

import io.github.pzhin.sfqd.SfqdScheduler;

final class Consumer {
    private SfqdScheduler<String, String, String> scheduler;
}
EOF

LC_ALL=C LANG=C TZ=UTC "${REPOSITORY_ROOT}/mvnw" \
  --batch-mode --no-transfer-progress --update-snapshots \
  -Dmaven.repo.local="${CONSUMER_LOCAL_REPOSITORY}" \
  -f "${CONSUMER_ROOT}/pom.xml" clean compile

readonly RESOLVED_PARENT="${CONSUMER_LOCAL_REPOSITORY}/io/github/pzhin/sfqd-java-parent/0.1.0-SNAPSHOT"
readonly RESOLVED_CORE="${CONSUMER_LOCAL_REPOSITORY}/io/github/pzhin/sfqd-core/0.1.0-SNAPSHOT"
if [[ ! -f "${RESOLVED_PARENT}/_remote.repositories" ]]; then
  fail "fresh consumer did not resolve the published parent"
fi
if [[ ! -f "${RESOLVED_CORE}/_remote.repositories" ]]; then
  fail "fresh consumer did not resolve the published core artifact"
fi
if ! grep -q 'publication-smoke' "${RESOLVED_PARENT}/_remote.repositories"; then
  fail "fresh consumer parent did not come from the temporary deployment repository"
fi
if ! grep -q 'publication-smoke' "${RESOLVED_CORE}/_remote.repositories"; then
  fail "fresh consumer core artifact did not come from the temporary deployment repository"
fi
if [[ ! -f "${CONSUMER_ROOT}/target/classes/smoke/Consumer.class" ]]; then
  fail "fresh consumer did not compile against the deployed core artifact"
fi

echo "PUBLICATION_TOPOLOGY PASS parent=published core=published auxiliary=skipped consumer=resolved"
