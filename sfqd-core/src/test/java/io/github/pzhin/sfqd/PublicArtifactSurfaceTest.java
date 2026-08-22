package io.github.pzhin.sfqd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.pzhin.sfqd.build.PublicApiManifest;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class PublicArtifactSurfaceTest {
    @Test
    void exposesExactlyTheSpecifiedPublicApi() throws IOException, URISyntaxException, ClassNotFoundException {
        Path classesRoot = Path.of(SfqdScheduler.class.getProtectionDomain()
                .getCodeSource().getLocation().toURI());
        assertTrue(Files.isDirectory(classesRoot), "core tests must run from the compiled classes directory");

        String expected = PublicApiManifest.read(Path.of("src/main/api/public-api.txt"));
        String actual = PublicApiManifest.describeDirectory(SfqdScheduler.class.getClassLoader(), classesRoot);

        assertEquals(expected, actual, () -> "public API signature manifest differs"
                + PublicApiManifest.difference(expected, actual));
    }
}
