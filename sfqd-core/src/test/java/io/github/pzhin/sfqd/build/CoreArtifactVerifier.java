package io.github.pzhin.sfqd.build;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/** Cross-platform mutation and verification of the three published core JARs. */
public final class CoreArtifactVerifier {
    private static final String LICENSE_ENTRY = "META-INF/LICENSE";
    private static final String SCHEDULER_PATH = "io/github/pzhin/sfqd/SfqdScheduler";
    private static final Set<String> EXPECTED_PUBLIC_TYPES = Set.of(
            "io.github.pzhin.sfqd.CancelResult",
            "io.github.pzhin.sfqd.CancellationAccounting",
            "io.github.pzhin.sfqd.CloseFlowResult",
            "io.github.pzhin.sfqd.CompletionResult",
            "io.github.pzhin.sfqd.Dispatch",
            "io.github.pzhin.sfqd.EnqueueResult",
            "io.github.pzhin.sfqd.EnqueueResult$Accepted",
            "io.github.pzhin.sfqd.EnqueueResult$Rejected",
            "io.github.pzhin.sfqd.FlowHandle",
            "io.github.pzhin.sfqd.FlowSnapshot",
            "io.github.pzhin.sfqd.JobHandle",
            "io.github.pzhin.sfqd.RegisterFlowResult",
            "io.github.pzhin.sfqd.RegisterFlowResult$Registered",
            "io.github.pzhin.sfqd.RegisterFlowResult$Rejected",
            "io.github.pzhin.sfqd.SchedulerConfig",
            "io.github.pzhin.sfqd.SchedulerSnapshot",
            "io.github.pzhin.sfqd.SfqdScheduler");

    private CoreArtifactVerifier() {
    }

    /**
     * Adds the project license to attached artifacts and validates their complete publication surface.
     *
     * @param arguments project directory, target directory, license, output timestamp, and Java release
     * @throws IOException when an artifact cannot be read, updated, or verified
     * @throws ClassNotFoundException when an archived class cannot be loaded
     */
    public static void main(String[] arguments) throws IOException, ClassNotFoundException {
        if (arguments.length == 2 && arguments[0].equals("--discover")) {
            Path targetDirectory = Path.of(arguments[1]).toAbsolutePath().normalize();
            Map<ArtifactRole, Path> artifacts = classifyArtifacts(targetDirectory);
            for (ArtifactRole role : ArtifactRole.values()) {
                Path reportedPath = targetDirectory.resolve(fileName(artifact(artifacts, role)));
                System.out.println(role.commandName + "\t" + reportedPath);
            }
            return;
        }
        if (arguments.length != 5) {
            throw new IllegalArgumentException(
                    "expected PROJECT_DIRECTORY TARGET_DIRECTORY LICENSE OUTPUT_TIMESTAMP JAVA_RELEASE");
        }
        Path projectDirectory = Path.of(arguments[0]);
        Path targetDirectory = Path.of(arguments[1]);
        Path licensePath = Path.of(arguments[2]);
        Instant outputTimestamp = Instant.parse(arguments[3]);
        int javaRelease = Integer.parseInt(arguments[4]);

        require(Files.isDirectory(projectDirectory.resolve("src/main/java")),
                "main source directory is missing");
        require(Files.isDirectory(targetDirectory.resolve("classes")),
                "compiled main output is missing");
        require(Files.isRegularFile(licensePath), "project LICENSE is missing");

        selfTestDiscovery(outputTimestamp);
        Map<ArtifactRole, Path> artifacts = classifyArtifacts(targetDirectory);
        byte[] license = Files.readAllBytes(licensePath);
        rewriteWithLicense(artifact(artifacts, ArtifactRole.SOURCES), license, outputTimestamp);
        rewriteWithLicense(artifact(artifacts, ArtifactRole.JAVADOC), license, outputTimestamp);
        verifyArtifacts(projectDirectory, targetDirectory, artifacts, license, javaRelease);
        System.out.printf("CORE_ARTIFACTS PASS binary=%s sources=%s javadoc=%s release=%d%n",
                fileName(artifact(artifacts, ArtifactRole.BINARY)),
                fileName(artifact(artifacts, ArtifactRole.SOURCES)),
                fileName(artifact(artifacts, ArtifactRole.JAVADOC)), javaRelease);
    }

    private static void verifyArtifacts(
            Path projectDirectory,
            Path targetDirectory,
            Map<ArtifactRole, Path> artifacts,
            byte[] license,
            int javaRelease) throws IOException, ClassNotFoundException {
        Path binaryArtifact = artifact(artifacts, ArtifactRole.BINARY);
        Path sourcesArtifact = artifact(artifacts, ArtifactRole.SOURCES);
        Path javadocArtifact = artifact(artifacts, ArtifactRole.JAVADOC);
        Set<String> binaryEntries = archiveEntries(binaryArtifact);
        Set<String> sourceEntries = archiveEntries(sourcesArtifact);
        Set<String> javadocEntries = archiveEntries(javadocArtifact);

        Set<String> expectedBinaryEntries = relativeFiles(targetDirectory.resolve("classes"), ".class");
        assertEqual("binary JAR classes differ from the compiled main output",
                expectedBinaryEntries, entriesEndingWith(binaryEntries, ".class"));

        Path sourceRoot = projectDirectory.resolve("src/main/java");
        Set<String> expectedSourceEntries = relativeFiles(sourceRoot, ".java");
        assertEqual("sources JAR differs from the main source tree",
                expectedSourceEntries, entriesEndingWith(sourceEntries, ".java"));

        verifyLicense(binaryArtifact, license, "binary");
        verifyLicense(sourcesArtifact, license, "sources");
        verifyLicense(javadocArtifact, license, "JavaDoc");
        verifyClassRelease(binaryArtifact, binaryEntries, javaRelease);
        verifyPublicTypes(binaryArtifact, binaryEntries);
        verifyJavadocTypes(javadocEntries);
        verifyNoToolingPackage(artifacts);
    }

    private static void verifyClassRelease(Path binaryArtifact, Set<String> entries, int javaRelease)
            throws IOException {
        int expectedMajorVersion = javaRelease + 44;
        try (ZipFile archive = new ZipFile(binaryArtifact.toFile())) {
            for (String entryName : entriesEndingWith(entries, ".class")) {
                ZipEntry entry = Objects.requireNonNull(archive.getEntry(entryName),
                        "class entry disappeared: " + entryName);
                try (DataInputStream input = new DataInputStream(
                        new BufferedInputStream(archive.getInputStream(entry)))) {
                    require(input.readInt() == 0xCAFEBABE, "invalid class file header: " + entryName);
                    input.readUnsignedShort();
                    int actualMajorVersion = input.readUnsignedShort();
                    require(actualMajorVersion == expectedMajorVersion,
                            "binary JAR class release differs from --release " + javaRelease + ": " + entryName);
                }
            }
        }
    }

    private static void verifyPublicTypes(Path binaryArtifact, Set<String> binaryEntries)
            throws IOException, ClassNotFoundException {
        Set<String> actualPublicTypes = new TreeSet<>();
        URL[] urls = {binaryArtifact.toUri().toURL()};
        try (URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            for (String entry : entriesEndingWith(binaryEntries, ".class")) {
                if (entry.equals("module-info.class") || entry.endsWith("/package-info.class")) {
                    continue;
                }
                String binaryName = entry.substring(0, entry.length() - ".class".length()).replace('/', '.');
                Class<?> type = Class.forName(binaryName, false, loader);
                if (Modifier.isPublic(type.getModifiers())) {
                    actualPublicTypes.add(binaryName);
                }
            }
        }
        assertEqual("binary JAR public type surface differs from the specified API",
                EXPECTED_PUBLIC_TYPES, actualPublicTypes);
    }

    private static void verifyJavadocTypes(Set<String> entries) {
        Set<String> expected = new TreeSet<>();
        for (String binaryName : EXPECTED_PUBLIC_TYPES) {
            String simpleName = binaryName.substring("io.github.pzhin.sfqd.".length()).replace('$', '.');
            expected.add("io/github/pzhin/sfqd/" + simpleName + ".html");
        }
        Set<String> actual = new TreeSet<>();
        for (String entry : entries) {
            if (entry.matches("io/github/pzhin/sfqd/[^/]+\\.html")
                    && !entry.matches("io/github/pzhin/sfqd/package-(summary|tree|use)\\.html")) {
                actual.add(entry);
            }
        }
        assertEqual("JavaDoc JAR type pages differ from the specified public API", expected, actual);
    }

    private static void verifyNoToolingPackage(Map<ArtifactRole, Path> artifacts) throws IOException {
        for (Path artifact : artifacts.values()) {
            for (String entry : archiveEntries(artifact)) {
                require(!entry.startsWith("io/github/pzhin/sfqd/tooling/"),
                        "obsolete tooling package is present in " + artifact);
            }
        }
    }

    private static void verifyLicense(Path artifact, byte[] expected, String role) throws IOException {
        try (ZipFile archive = new ZipFile(artifact.toFile())) {
            ZipEntry entry = Objects.requireNonNull(archive.getEntry(LICENSE_ENTRY),
                    role + " JAR does not contain " + LICENSE_ENTRY);
            try (InputStream input = archive.getInputStream(entry)) {
                require(java.util.Arrays.equals(expected, input.readAllBytes()),
                        role + " JAR license differs from the project LICENSE");
            }
        }
    }

    private static void rewriteWithLicense(Path archive, byte[] license, Instant timestamp) throws IOException {
        Path parent = Objects.requireNonNull(archive.getParent(), "artifact has no parent directory: " + archive);
        Path temporary = Files.createTempFile(parent, fileName(archive), ".tmp");
        boolean moved = false;
        try {
            try (ZipFile input = new ZipFile(archive.toFile());
                    OutputStream fileOutput = new BufferedOutputStream(Files.newOutputStream(temporary));
                    JarOutputStream output = new JarOutputStream(fileOutput)) {
                Enumeration<? extends ZipEntry> entries = input.entries();
                while (entries.hasMoreElements()) {
                    ZipEntry original = entries.nextElement();
                    if (original.getName().equals(LICENSE_ENTRY)) {
                        continue;
                    }
                    JarEntry replacement = new JarEntry(original);
                    output.putNextEntry(replacement);
                    if (!original.isDirectory()) {
                        try (InputStream entryInput = input.getInputStream(original)) {
                            entryInput.transferTo(output);
                        }
                    }
                    output.closeEntry();
                }
                JarEntry licenseEntry = new JarEntry(LICENSE_ENTRY);
                licenseEntry.setLastModifiedTime(FileTime.from(timestamp));
                output.putNextEntry(licenseEntry);
                output.write(license);
                output.closeEntry();
            }
            moveAtomicallyWhenSupported(temporary, archive);
            moved = true;
        } finally {
            if (!moved) {
                Files.deleteIfExists(temporary);
            }
        }
    }

    private static void moveAtomicallyWhenSupported(Path source, Path destination) throws IOException {
        try {
            Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static Map<ArtifactRole, Path> classifyArtifacts(Path targetDirectory) throws IOException {
        List<Path> candidates;
        try (Stream<Path> files = Files.list(targetDirectory)) {
            candidates = files
                    .filter(Files::isRegularFile)
                    .filter(path -> fileName(path).endsWith(".jar"))
                    .sorted()
                    .toList();
        }
        require(candidates.size() == 3,
                "expected exactly three core JAR artifacts, found " + candidates.size());

        Map<ArtifactRole, Path> artifacts = new EnumMap<>(ArtifactRole.class);
        for (Path candidate : candidates) {
            Set<String> entries = archiveEntries(candidate);
            List<ArtifactRole> matches = new ArrayList<>();
            if (entries.contains(SCHEDULER_PATH + ".class")) {
                matches.add(ArtifactRole.BINARY);
            }
            if (entries.contains(SCHEDULER_PATH + ".java")) {
                matches.add(ArtifactRole.SOURCES);
            }
            if (entries.contains(SCHEDULER_PATH + ".html") && entries.contains("element-list")) {
                matches.add(ArtifactRole.JAVADOC);
            }
            require(matches.size() == 1,
                    "core artifact must match exactly one content identity: " + candidate);
            ArtifactRole role = matches.getFirst();
            require(artifacts.put(role, candidate) == null, "duplicate core " + role.displayName + " artifact");
        }
        for (ArtifactRole role : ArtifactRole.values()) {
            require(artifacts.containsKey(role), "core " + role.displayName + " artifact is missing");
        }
        return artifacts;
    }

    private static Set<String> archiveEntries(Path archive) throws IOException {
        Set<String> entries = new HashSet<>();
        try (ZipFile zipFile = new ZipFile(archive.toFile())) {
            Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
            while (enumeration.hasMoreElements()) {
                entries.add(enumeration.nextElement().getName());
            }
        }
        return entries;
    }

    private static Set<String> relativeFiles(Path root, String suffix) throws IOException {
        Set<String> entries = new TreeSet<>();
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(Files::isRegularFile)
                    .filter(path -> fileName(path).endsWith(suffix))
                    .map(root::relativize)
                    .map(Path::toString)
                    .map(name -> name.replace(root.getFileSystem().getSeparator(), "/"))
                    .forEach(entries::add);
        }
        return entries;
    }

    private static Set<String> entriesEndingWith(Set<String> entries, String suffix) {
        Set<String> matches = new TreeSet<>();
        for (String entry : entries) {
            if (entry.endsWith(suffix)) {
                matches.add(entry);
            }
        }
        return matches;
    }

    private static void selfTestDiscovery(Instant timestamp) throws IOException {
        Path temporary = Files.createTempDirectory("sfqd-artifact-discovery-");
        try {
            writeMarkerJar(temporary.resolve("sfqd-core-9.8.7-sources.jar"),
                    SCHEDULER_PATH + ".class", timestamp);
            writeMarkerJar(temporary.resolve("sfqd-core-9.8.7-sources-sources.jar"),
                    SCHEDULER_PATH + ".java", timestamp);
            writeMarkerJar(temporary.resolve("sfqd-core-9.8.7-sources-javadoc.jar"),
                    SCHEDULER_PATH + ".html", timestamp, "element-list");
            Map<ArtifactRole, Path> artifacts = classifyArtifacts(temporary);
            require(fileName(artifact(artifacts, ArtifactRole.BINARY))
                            .equals("sfqd-core-9.8.7-sources.jar"),
                    "content discovery confused a binary whose version ends in -sources");
            require(fileName(artifact(artifacts, ArtifactRole.SOURCES))
                            .equals("sfqd-core-9.8.7-sources-sources.jar"),
                    "content discovery did not identify the sources classifier");
            require(fileName(artifact(artifacts, ArtifactRole.JAVADOC))
                            .equals("sfqd-core-9.8.7-sources-javadoc.jar"),
                    "content discovery did not identify the JavaDoc classifier");
        } finally {
            deleteTree(temporary);
        }
        System.out.println("CORE_ARTIFACT_DISCOVERY_SELF_TEST PASS version=9.8.7-sources");
    }

    private static void writeMarkerJar(Path path, String marker, Instant timestamp, String... extras)
            throws IOException {
        try (OutputStream fileOutput = new BufferedOutputStream(Files.newOutputStream(path));
                JarOutputStream output = new JarOutputStream(fileOutput)) {
            List<String> entries = new ArrayList<>();
            entries.add(marker);
            entries.addAll(List.of(extras));
            for (String entryName : entries) {
                JarEntry entry = new JarEntry(entryName);
                entry.setLastModifiedTime(FileTime.from(timestamp));
                output.putNextEntry(entry);
                output.closeEntry();
            }
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

    private static void assertEqual(String message, Set<String> expected, Set<String> actual) {
        require(expected.equals(actual), message + "; expected=" + expected + "; actual=" + actual);
    }

    private static Path artifact(Map<ArtifactRole, Path> artifacts, ArtifactRole role) {
        return Objects.requireNonNull(artifacts.get(role), "core " + role.displayName + " artifact is missing");
    }

    private static String fileName(Path path) {
        Path name = Objects.requireNonNull(path.getFileName(), "path has no file name: " + path);
        return name.toString();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private enum ArtifactRole {
        BINARY("binary", "binary"),
        SOURCES("sources", "sources"),
        JAVADOC("javadoc", "JavaDoc");

        private final String commandName;
        private final String displayName;

        ArtifactRole(String commandName, String displayName) {
            this.commandName = commandName;
            this.displayName = displayName;
        }
    }
}
