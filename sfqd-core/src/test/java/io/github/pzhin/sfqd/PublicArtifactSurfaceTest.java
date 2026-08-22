package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

final class PublicArtifactSurfaceTest {
    private static final Set<String> EXPECTED_PUBLIC_TYPES = Set.of(
            "io.github.pzhin.sfqd.CancelResult",
            "io.github.pzhin.sfqd.CloseFlowResult",
            "io.github.pzhin.sfqd.CompletionResult",
            "io.github.pzhin.sfqd.Dispatch",
            "io.github.pzhin.sfqd.EnqueueResult",
            "io.github.pzhin.sfqd.EnqueueResult$Accepted",
            "io.github.pzhin.sfqd.EnqueueResult$Rejected",
            "io.github.pzhin.sfqd.FlowHandle",
            "io.github.pzhin.sfqd.JobHandle",
            "io.github.pzhin.sfqd.RegisterFlowResult",
            "io.github.pzhin.sfqd.RegisterFlowResult$Registered",
            "io.github.pzhin.sfqd.RegisterFlowResult$Rejected",
            "io.github.pzhin.sfqd.SchedulerConfig",
            "io.github.pzhin.sfqd.SchedulerSnapshot",
            "io.github.pzhin.sfqd.SfqdScheduler");

    @Test
    void exposesOnlyTheSpecifiedPublicTypes() throws IOException, URISyntaxException {
        Path classesRoot = Path.of(SfqdScheduler.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        assertTrue(Files.isDirectory(classesRoot), "core tests must run from the compiled classes directory");

        Set<String> actualPublicTypes;
        try (Stream<Path> files = Files.walk(classesRoot)) {
            actualPublicTypes = files
                    .filter(Files::isRegularFile)
                    .filter(PublicArtifactSurfaceTest::isTypeClassFile)
                    .map(path -> binaryName(classesRoot, path))
                    .filter(PublicArtifactSurfaceTest::isPublic)
                    .collect(Collectors.toUnmodifiableSet());
        }

        assertEquals(EXPECTED_PUBLIC_TYPES, actualPublicTypes);
    }

    private static boolean isTypeClassFile(Path path) {
        Path fileName = path.getFileName();
        if (fileName == null) {
            return false;
        }
        String name = fileName.toString();
        return name.endsWith(".class")
                && !name.equals("module-info.class")
                && !name.equals("package-info.class");
    }

    private static String binaryName(Path classesRoot, Path classFile) {
        String relativeName = classesRoot.relativize(classFile).toString();
        return relativeName.substring(0, relativeName.length() - ".class".length())
                .replace(classFile.getFileSystem().getSeparator(), ".");
    }

    private static boolean isPublic(String binaryName) {
        try {
            Class<?> type = Class.forName(binaryName, false, SfqdScheduler.class.getClassLoader());
            return Modifier.isPublic(type.getModifiers());
        } catch (ClassNotFoundException impossible) {
            throw new AssertionError("compiled class cannot be loaded: " + binaryName, impossible);
        }
    }
}
