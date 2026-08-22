package io.github.pzhin.sfqd.build;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Generates and reads the canonical public API signature manifest. */
public final class PublicApiManifest {
    private static final String HEADER = """
            # SFQ(D) core public API signature manifest
            # Generated from compiled classes by PublicApiManifest; update only for an intentional API change.
            format=1
            """;

    private PublicApiManifest() {
    }

    /**
     * Prints a manifest for a compiled classes directory.
     *
     * @param arguments one compiled classes directory
     * @throws IOException when the directory cannot be read
     * @throws ClassNotFoundException when a compiled class cannot be loaded
     */
    public static void main(String[] arguments) throws IOException, ClassNotFoundException {
        if (arguments.length != 1) {
            throw new IllegalArgumentException("expected one compiled classes directory");
        }
        Path classesRoot = Path.of(arguments[0]).toAbsolutePath().normalize();
        URL[] urls = {classesRoot.toUri().toURL()};
        try (URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            System.out.print(describe(loader, binaryNames(classesRoot)));
        }
    }

    /**
     * Describes every public type and its declared public or protected members.
     *
     * @param loader isolated loader for the classes being described
     * @param binaryNames all binary class names in the artifact
     * @return canonical manifest text with LF line endings
     * @throws ClassNotFoundException when an artifact class cannot be loaded
     */
    public static String describe(ClassLoader loader, Collection<String> binaryNames)
            throws ClassNotFoundException {
        Set<String> typeLines = new TreeSet<>();
        Set<String> memberLines = new TreeSet<>();
        for (String binaryName : new TreeSet<>(binaryNames)) {
            Class<?> type = Class.forName(binaryName, false, loader);
            if (!Modifier.isPublic(type.getModifiers())) {
                continue;
            }
            typeLines.add(describeType(type));
            describeConstructors(type, memberLines);
            describeFields(type, memberLines);
            describeMethods(type, memberLines);
            describeRecordComponents(type, memberLines);
        }
        return HEADER + '\n' + String.join("\n", typeLines) + "\n\n"
                + String.join("\n", memberLines) + '\n';
    }

    /**
     * Describes all compiled types below a classes directory.
     *
     * @param loader loader for the classes being described
     * @param classesRoot compiled classes directory
     * @return canonical manifest text with LF line endings
     * @throws IOException when the directory cannot be read
     * @throws ClassNotFoundException when a compiled class cannot be loaded
     */
    public static String describeDirectory(ClassLoader loader, Path classesRoot)
            throws IOException, ClassNotFoundException {
        return describe(loader, binaryNames(classesRoot));
    }

    /**
     * Reads a checked-in manifest and normalizes platform line endings.
     *
     * @param path manifest path
     * @return manifest text with LF line endings
     * @throws IOException when the manifest cannot be read
     */
    public static String read(Path path) throws IOException {
        return Files.readString(path).replace("\r\n", "\n");
    }

    /**
     * Extracts public binary type names from canonical manifest text.
     *
     * @param manifest canonical manifest text
     * @return sorted public binary names
     */
    public static Set<String> publicTypeNames(String manifest) {
        Set<String> names = new TreeSet<>();
        for (String line : manifest.split("\n")) {
            if (line.startsWith("TYPE name=")) {
                int end = line.indexOf(' ', "TYPE name=".length());
                if (end < 0) {
                    throw new IllegalArgumentException("malformed TYPE manifest line: " + line);
                }
                names.add(line.substring("TYPE name=".length(), end));
            }
        }
        if (names.isEmpty()) {
            throw new IllegalArgumentException("manifest contains no public types");
        }
        return names;
    }

    /**
     * Reports missing and unexpected canonical lines.
     *
     * @param expected checked-in manifest
     * @param actual generated manifest
     * @return compact difference description
     */
    public static String difference(String expected, String actual) {
        Set<String> missing = manifestLines(expected);
        Set<String> unexpected = manifestLines(actual);
        missing.removeAll(manifestLines(actual));
        unexpected.removeAll(manifestLines(expected));
        if (missing.isEmpty() && unexpected.isEmpty() && !expected.equals(actual)) {
            return firstOrderedDifference(expected, actual);
        }
        return "; missing=" + missing + "; unexpected=" + unexpected;
    }

    private static String firstOrderedDifference(String expected, String actual) {
        String[] expectedLines = expected.split("\n", -1);
        String[] actualLines = actual.split("\n", -1);
        int sharedLength = Math.min(expectedLines.length, actualLines.length);
        for (int index = 0; index < sharedLength; index++) {
            if (!expectedLines[index].equals(actualLines[index])) {
                return "; firstDifferenceLine=" + (index + 1)
                        + "; expected=" + expectedLines[index]
                        + "; actual=" + actualLines[index];
            }
        }
        return "; lineCount expected=" + expectedLines.length + "; actual=" + actualLines.length;
    }

    private static Collection<String> binaryNames(Path classesRoot) throws IOException {
        List<String> names = new ArrayList<>();
        try (Stream<Path> files = Files.walk(classesRoot)) {
            files.filter(Files::isRegularFile)
                    .filter(PublicApiManifest::isTypeClassFile)
                    .map(classesRoot::relativize)
                    .map(Path::toString)
                    .map(name -> name.substring(0, name.length() - ".class".length()))
                    .map(name -> name.replace(classesRoot.getFileSystem().getSeparator(), "."))
                    .forEach(names::add);
        }
        return names;
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

    private static String describeType(Class<?> type) {
        String superclass = type.getGenericSuperclass() == null
                ? "-" : typeName(type.getGenericSuperclass());
        return "TYPE name=" + type.getName()
                + " kind=" + kind(type)
                + " modifiers=" + typeModifiers(type)
                + " typeParameters=" + typeParameters(type.getTypeParameters())
                + " superclass=" + superclass
                + " interfaces=" + types(type.getGenericInterfaces())
                + " permits=" + permittedSubclasses(type)
                + " synthetic=" + type.isSynthetic();
    }

    private static void describeConstructors(Class<?> type, Set<String> lines) {
        for (Constructor<?> constructor : type.getDeclaredConstructors()) {
            if (isVisible(constructor.getModifiers())) {
                lines.add("CONSTRUCTOR owner=" + type.getName()
                        + " modifiers=" + executableModifiers(constructor)
                        + " typeParameters=" + typeParameters(constructor.getTypeParameters())
                        + " parameters=" + types(constructor.getGenericParameterTypes())
                        + " throws=" + types(constructor.getGenericExceptionTypes()));
            }
        }
    }

    private static void describeFields(Class<?> type, Set<String> lines) {
        for (Field field : type.getDeclaredFields()) {
            if (isVisible(field.getModifiers())) {
                lines.add("FIELD owner=" + type.getName()
                        + " name=" + field.getName()
                        + " modifiers=" + fieldModifiers(field)
                        + " type=" + typeName(field.getGenericType())
                        + " enumConstant=" + field.isEnumConstant()
                        + " synthetic=" + field.isSynthetic());
            }
        }
    }

    private static void describeMethods(Class<?> type, Set<String> lines) {
        for (Method method : type.getDeclaredMethods()) {
            if (isVisible(method.getModifiers())) {
                lines.add("METHOD owner=" + type.getName()
                        + " name=" + method.getName()
                        + " modifiers=" + methodModifiers(method)
                        + " typeParameters=" + typeParameters(method.getTypeParameters())
                        + " return=" + typeName(method.getGenericReturnType())
                        + " parameters=" + types(method.getGenericParameterTypes())
                        + " throws=" + types(method.getGenericExceptionTypes()));
            }
        }
    }

    private static void describeRecordComponents(Class<?> type, Set<String> lines) {
        RecordComponent[] components = type.getRecordComponents();
        if (components == null) {
            return;
        }
        for (RecordComponent component : components) {
            lines.add("RECORD_COMPONENT owner=" + type.getName()
                    + " name=" + component.getName()
                    + " type=" + typeName(component.getGenericType()));
        }
    }

    private static String kind(Class<?> type) {
        if (type.isAnnotation()) {
            return "annotation";
        }
        if (type.isEnum()) {
            return "enum";
        }
        if (type.isRecord()) {
            return "record";
        }
        if (type.isInterface()) {
            return "interface";
        }
        return "class";
    }

    private static String typeModifiers(Class<?> type) {
        List<String> modifiers = baseModifiers(type.getModifiers());
        if (type.isSealed()) {
            modifiers.add("sealed");
        } else if (!Modifier.isFinal(type.getModifiers()) && directlyExtendsSealedType(type)) {
            modifiers.add("non-sealed");
        }
        return list(modifiers);
    }

    private static boolean directlyExtendsSealedType(Class<?> type) {
        if (type.getSuperclass() != null && type.getSuperclass().isSealed()) {
            return true;
        }
        return Arrays.stream(type.getInterfaces()).anyMatch(Class::isSealed);
    }

    private static String fieldModifiers(Field field) {
        List<String> modifiers = baseModifiers(field.getModifiers());
        if (Modifier.isTransient(field.getModifiers())) {
            modifiers.add("transient");
        }
        if (Modifier.isVolatile(field.getModifiers())) {
            modifiers.add("volatile");
        }
        return list(modifiers);
    }

    private static String executableModifiers(Executable executable) {
        List<String> modifiers = baseModifiers(executable.getModifiers());
        if (executable instanceof Method method) {
            if (Modifier.isSynchronized(method.getModifiers())) {
                modifiers.add("synchronized");
            }
            if (Modifier.isNative(method.getModifiers())) {
                modifiers.add("native");
            }
            if (method.isDefault()) {
                modifiers.add("default");
            }
            if (method.isBridge()) {
                modifiers.add("bridge");
            }
        }
        if (executable.isVarArgs()) {
            modifiers.add("varargs");
        }
        if (executable.isSynthetic()) {
            modifiers.add("synthetic");
        }
        return list(modifiers);
    }

    private static String methodModifiers(Method method) {
        return executableModifiers(method);
    }

    private static List<String> baseModifiers(int modifiers) {
        List<String> result = new ArrayList<>();
        if (Modifier.isPublic(modifiers)) {
            result.add("public");
        } else if (Modifier.isProtected(modifiers)) {
            result.add("protected");
        } else if (Modifier.isPrivate(modifiers)) {
            result.add("private");
        }
        if (Modifier.isAbstract(modifiers)) {
            result.add("abstract");
        }
        if (Modifier.isStatic(modifiers)) {
            result.add("static");
        }
        if (Modifier.isFinal(modifiers)) {
            result.add("final");
        }
        if (Modifier.isStrict(modifiers)) {
            result.add("strictfp");
        }
        return result;
    }

    private static String permittedSubclasses(Class<?> type) {
        Class<?>[] subclasses = type.getPermittedSubclasses();
        if (subclasses == null) {
            return "[]";
        }
        return Arrays.stream(subclasses)
                .map(Class::getName)
                .sorted()
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private static String typeParameters(TypeVariable<?>[] variables) {
        return Arrays.stream(variables)
                .map(PublicApiManifest::typeParameter)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private static String typeParameter(TypeVariable<?> variable) {
        Type[] bounds = variable.getBounds();
        if (bounds.length == 1 && bounds[0].equals(Object.class)) {
            return variable.getName();
        }
        return variable.getName() + " extends " + Arrays.stream(bounds)
                .map(PublicApiManifest::typeName)
                .collect(Collectors.joining(" & "));
    }

    private static String types(Type[] types) {
        return Arrays.stream(types)
                .map(PublicApiManifest::typeName)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private static String typeName(Type type) {
        return type.getTypeName();
    }

    private static String list(List<String> values) {
        return '[' + String.join(", ", values) + ']';
    }

    private static boolean isVisible(int modifiers) {
        return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
    }

    private static Set<String> manifestLines(String manifest) {
        return new TreeSet<>(Arrays.asList(manifest.split("\n")));
    }
}
