/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.annotation;

import static com.alibaba.graphscope.annotation.Utils.cpp2Java;
import static com.alibaba.graphscope.annotation.Utils.java2Cpp;
import static com.google.testing.compile.Compiler.javac;

import com.alibaba.fastffi.FFIMirror;
import com.alibaba.fastffi.FFINameSpace;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.annotation.AnnotationProcessor;
import com.google.testing.compile.Compilation;
import com.google.testing.compile.JavaFileObjects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.tools.JavaFileObject;

/**
 * This class accept
 *
 * <p>1) classpath 2) output directory 3) graph template string Then scan all class, for all class
 * annotated with FFIGen, FFIMirror, do the code generation.
 */
public class GraphScopeAppScanner {

    private static Logger logger = LoggerFactory.getLogger(GraphScopeAppScanner.class.getName());
    private boolean javaFragName;
    private String classpath;
    private String configPath;
    private String outputDirectory;
    private String graphTemplateStr;
    private String vertexDataType;
    private GraphConfig graphConfig;
    private List<File> classPathList;
    private URLClassLoader appClassLoader;
    // java name to foreign name
    private Map<String, String> ffiMirrors = new HashMap<>();

    private GraphScopeAppScanner(
            String classpath,
            String configPath,
            String outputDirectory,
            String graphTemplateStr,
            String vertexDataType,
            boolean javaFragName) {
        this.classpath = classpath;
        this.configPath = configPath;
        this.outputDirectory = outputDirectory;
        this.classPathList = parseClassPath(classpath);
        this.appClassLoader =
                new URLClassLoader(
                        classPathList.stream()
                                .map(
                                        cp -> {
                                            try {
                                                return cp.toURI().toURL();
                                            } catch (MalformedURLException e) {
                                                throw new IllegalArgumentException(
                                                        "Not a valid path: " + cp);
                                            }
                                        })
                                .toArray(URL[]::new),
                        ClassLoader.getSystemClassLoader());
        this.graphTemplateStr = graphTemplateStr;
        if (this.graphTemplateStr.startsWith("\"") && this.graphTemplateStr.endsWith("\"")) {
            this.graphTemplateStr =
                    this.graphTemplateStr.substring(1, this.graphTemplateStr.length() - 1);
        }
        logger.info("Graph template string {}", graphTemplateStr);
        this.vertexDataType = vertexDataType;
        this.javaFragName = javaFragName;
    }

    /**
     * We define the same interface for the generation of graphScope of grape. For graphScope,
     * config path is not used, which means we don't need the configuration file, messageTypes are
     * all from FFIMirror,(later we should scan the jar to find all usage of the message send
     * function) For Grape, we still run the main method to generate the configuration file.
     *
     * @param classpath       java class path
     * @param outputDirectory where to output
     * @return absolute output path
     */
    public static String scanAppAndGenerate(
            String classpath,
            String outputDirectory,
            String graphTemplateString,
            String vertexDataType,
            boolean javaFragName) {
        return new GraphScopeAppScanner(
                        classpath,
                        "empty",
                        outputDirectory,
                        graphTemplateString,
                        vertexDataType,
                        javaFragName)
                .scanAppAndGenerateImpl();
    }

    static List<File> parseClassPath(String classpath) {
        return Arrays.stream(classpath.split(File.pathSeparator))
                .map(c -> Paths.get(c).toFile())
                .collect(Collectors.toList());
    }

    private static String readProperty(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing " + key + " in graph config.");
        }
        return value;
    }

    private String scanAppAndGenerateImpl() {
        // readConfig();
        // for all ffiMirror we suppose they are all possible to appear in messageManager
        collectFFIMirror();
        StringBuilder sb = new StringBuilder();
        for (String str : ffiMirrors.keySet()) {
            sb.append(ffiMirrors.get(str) + "=" + str + ",");
        }
        // add support for doubleMsg and LongMsg
        //
        sb.append("gs::DoubleMsg=com.alibaba.graphscope.parallel.message.DoubleMsg,");
        sb.append("gs::LongMsg=com.alibaba.graphscope.parallel.message.LongMsg,");
        sb.append("grape::EmptyType=com.alibaba.graphscope.ds.EmptyType,");
        String temp = sb.toString();
        String messageTypes = "";
        if (!temp.isEmpty()) {
            messageTypes = temp.substring(0, temp.length() - 1);
        }
        // create a dummy graphConfig, oid type should never be used.
        logger.info("message types:" + messageTypes);

        String[] parsed = parseGraphTemplateStr(this.graphTemplateStr);
        if (Objects.isNull(parsed)) {
            logger.info("No generation!");
            return this.outputDirectory;
        }
        if (Objects.nonNull(parsed) && parsed.length == 5) {
            if (javaFragName) {
                graphConfig =
                        new GraphConfig(
                                parsed[1],
                                parsed[2],
                                parsed[3],
                                parsed[4],
                                messageTypes,
                                parsed[0],
                                java2Cpp(parsed[1], true),
                                java2Cpp(parsed[2], false),
                                java2Cpp(parsed[3], true),
                                java2Cpp(parsed[4], true),
                                vertexDataType);
            } else {
                graphConfig =
                        new GraphConfig(
                                cpp2Java(parsed[1]),
                                cpp2Java(parsed[2]),
                                cpp2Java(parsed[3]),
                                cpp2Java(parsed[4]),
                                messageTypes,
                                parsed[0],
                                parsed[1],
                                parsed[2],
                                parsed[3],
                                parsed[4],
                                vertexDataType);
            }

        } else {
            graphConfig =
                    new GraphConfig(
                            Long.class.getName(),
                            Long.class.getName(),
                            Long.class.getName(),
                            Double.class.getName(),
                            messageTypes,
                            "empty",
                            "",
                            "",
                            "",
                            "",
                            vertexDataType);
        }

        return generate();
    }

    private static String[] matchWithPattern(String graphTemplateStr, Pattern pattern) {

        return null;
    }
    // gs::ArrowProjectedFragment
    // can also be ArrowProjectedFragment<java.lang.Long,....>
    public static String[] parseGraphTemplateStr(String graphTemplateStr) {
        {
            Pattern pattern =
                    Pattern.compile(
                            "(.+)<(.+),(.+),(.+),(.+),vineyard::ArrowVertexMap<(.+),(.+)>,(.+)>");
            Matcher matcher = pattern.matcher(graphTemplateStr);
            if (matcher.find()) {
                return new String[] {
                    matcher.group(1),
                    matcher.group(2),
                    matcher.group(3),
                    matcher.group(4),
                    matcher.group(5)
                };
            }
        }
        {
            Pattern pattern = Pattern.compile("(.+)<(.+),(.+),(.+),(.+),(.+)>");
            Matcher matcher = pattern.matcher(graphTemplateStr);
            if (matcher.find()) {
                return new String[] {
                    matcher.group(1),
                    matcher.group(2),
                    matcher.group(3),
                    matcher.group(4),
                    matcher.group(5)
                };
            }
        }
        throw new IllegalStateException("unrecognized " + graphTemplateStr);
    }

    private String getFFIGenBatch() {
        List<String> ffiMirrorNames = new ArrayList<>(this.ffiMirrors.keySet());
        Collections.sort(ffiMirrorNames);
        if (ffiMirrorNames.isEmpty()) {
            logger.warn("No FFIMirror is detected.");
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("@FFIGenBatch({\n");
        sb.append("@FFIGen( type = \"").append(ffiMirrorNames.get(0)).append("\")");
        for (int i = 1; i < ffiMirrorNames.size(); i++) {
            String name = ffiMirrorNames.get(i);
            sb.append(",\n");
            sb.append("@FFIGen( type = \"").append(name).append("\")");
        }
        sb.append("})");
        return sb.toString();
    }

    private List<File> getClassPath() {
        String classpath = System.getProperty("java.class.path");
        if (classpath == null) {
            throw new IllegalStateException("Cannot get java.class.path");
        }
        return parseClassPath(classpath);
    }

    /**
     * Generate graphScope or grape. For graphScope, we only generate messageManagers functions.
     * Other classes and functions are provided in sdk
     *
     * @return
     */
    private String generate() {
        Random random = new Random();
        String packageName = "grape" + random.nextInt(Integer.MAX_VALUE);
        String classSimpleName = "GraphType" + random.nextInt(Integer.MAX_VALUE);
        String className = packageName + "." + classSimpleName;
        JavaFileObject file =
                JavaFileObjects.forSourceLines(
                        className,
                        String.format("package %s;", packageName),
                        "import com.alibaba.fastffi.*;",
                        "import com.alibaba.graphscope.annotation.*;",
                        getFFIGenBatch(),
                        "@GraphType(",
                        String.format("  oidType = %s.class,", graphConfig.oidType),
                        String.format("  vidType = %s.class,", graphConfig.vidType),
                        String.format("  vdataType = %s.class,", graphConfig.vdataType),
                        String.format("  edataType = %s.class,", graphConfig.edataType),
                        String.format("  cppOidType = \"%s\",", graphConfig.cppOidType),
                        String.format("  cppVidType = \"%s\",", graphConfig.cppVidType),
                        String.format("  cppVdataType = \"%s\",", graphConfig.cppVdataType),
                        String.format("  cppEdataType = \"%s\",", graphConfig.cppEdataType),
                        String.format("  fragType = \"%s\",", graphConfig.fragmentType),
                        String.format("  vertexDataType = \"%s\"", graphConfig.vertexDataType),
                        ")",
                        String.format("public class %s {}", classSimpleName));

        List<File> classpath = getClassPath();
        classpath.addAll(classPathList);
        if (graphConfig.messageTypes != null) {
            System.setProperty("grape.messageTypes", graphConfig.messageTypes);
        }
        Compilation compilation;
        logger.info("Generate for graphScope");
        compilation =
                javac().withClasspath(classpath)
                        .withProcessors(
                                new GraphScopeAnnotationProcessor(), new AnnotationProcessor())
                        .compile(file);

        Compilation.Status status = compilation.status();
        if (status == Compilation.Status.SUCCESS) {
            try {
                return writeFiles(compilation);
            } catch (IOException e) {
                throw new IllegalStateException("Oops, cannot write files ", e);
            }
        } else if (status == Compilation.Status.FAILURE) {
            compilation.errors().forEach(d -> logger.info("" + d));
            return null;
        } else {
            throw new IllegalStateException("Oops, should not reach here");
        }
    }

    private String writeFiles(Compilation compilation) throws IOException {
        // Path outputRoot = Files.createTempDirectory("grape-ffi");
        File outputRootDir = new File(this.outputDirectory);
        if (!outputRootDir.exists()) {
            outputRootDir.mkdirs();
        }
        Path outputRootPath = outputRootDir.toPath();
        // Path outputRoot = Files.createDirectories(Paths.get("grape-ffi"));
        outputRootPath = outputRootPath.toAbsolutePath();
        for (JavaFileObject generatedFile : compilation.generatedFiles()) {
            String path = generatedFile.toUri().getPath();
            String[] pathList = path.split("/");
            // My understanding is that URI path is always separated by '/'
            if (pathList.length == 0) {
                throw new IllegalStateException("Not a valid Uri path " + path);
            }
            pathList[0] = outputRootPath.toString();
            Path filePath = Paths.get(String.join(File.separator, pathList));
            Files.createDirectories(filePath.getParent());
            writeTo(generatedFile, filePath);
        }
        return outputRootPath.toAbsolutePath().toString();
    }

    private void writeTo(JavaFileObject fileObject, Path dst) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream(dst.toFile());
                InputStream inputStream = fileObject.openInputStream()) {
            byte[] buf = new byte[1024];
            int length;
            while ((length = inputStream.read(buf)) > 0) {
                fileOutputStream.write(buf, 0, length);
            }
        }
    }

    private void collectFFIMirror() {
        classPathList.forEach(cp -> collectFFIMirror(cp.toPath()));
    }

    public void collectFFIMirror(Path classPath) {
        try {
            if (classPath.toString().endsWith(".jar")) {
                Map<String, String> env = new HashMap<>();
                env.put("create", "false");
                URI root = URI.create("jar:file:" + classPath.toAbsolutePath());
                try (FileSystem fs = FileSystems.newFileSystem(root, env)) {
                    Path rootPath = fs.getPath("/");
                    Files.walkFileTree(rootPath, new ClassFileVisitor(rootPath));
                }
            } else {
                Files.walkFileTree(classPath, new ClassFileVisitor(classPath));
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot walk class path: " + classPath);
        }
    }

    class ClassFileVisitor extends SimpleFileVisitor<Path> {

        Path root;

        ClassFileVisitor(Path root) {
            this.root = root;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (attrs.isRegularFile()) {
                if (file.getFileName().toString().endsWith(".class")) {
                    Path relative = root.relativize(file);
                    String className = relative.toString();
                    className = className.substring(0, className.length() - 6).replace('/', '.');
                    // For FFIMirror found in grape-sdk, we don't count it in codegeneration
                    if (className.contains("com.alibaba.graphscope.stdcxx.FFISample")) {
                        logger.info("catch ffi mirror FFISample, skip generation" + className);
                        return FileVisitResult.CONTINUE;
                    }
                    try {
                        // Why we need to load the class into the JVM rather than check it using
                        // bytecode
                        // library such as asm? This is because we need to guarantee that all
                        // dependency is
                        // available during the code generation.
                        Class<?> clazz = Class.forName(className, false, appClassLoader);
                        if (!className.equals(clazz.getName())) {
                            throw new IllegalStateException(
                                    "Error: expected " + className + ", got " + clazz);
                        }
                        if (ffiMirrors.containsKey(className)) {
                            return FileVisitResult.CONTINUE;
                        }
                        FFIMirror ffiMirror = clazz.getAnnotation(FFIMirror.class);
                        if (ffiMirror != null) { // a FFIMirror
                            FFISynthetic ffiSynthetic = clazz.getAnnotation(FFISynthetic.class);
                            FFINameSpace ffiNameSpace = clazz.getAnnotation(FFINameSpace.class);
                            FFITypeAlias ffiTypeAlias = clazz.getAnnotation(FFITypeAlias.class);
                            if (ffiSynthetic == null) {
                                // not a generated class
                                ffiMirrors.put(
                                        className,
                                        ffiNameSpace.value() + "::" + ffiTypeAlias.value());
                            }
                        }
                    } catch (ClassNotFoundException | NoClassDefFoundError e) {
                        if (Main.ignoreError) {
                            if (Main.verbose) {
                                logger.error("WARNING: Cannot load class " + className);
                            }
                        } else {
                            throw new IllegalStateException(e);
                        }
                    } catch (IncompatibleClassChangeError e) {
                        if (Main.ignoreError) {
                            if (Main.verbose) {
                                logger.error("WARNING: incompatible class error " + className);
                            }
                        } else {
                            throw new IllegalStateException(e);
                        }
                    } catch (UnsupportedClassVersionError e) {
                        if (Main.ignoreError) {
                            if (Main.verbose) {
                                logger.error(
                                        "WARNING: unsupported class version"
                                                + className
                                                + e.getMessage());
                            }
                        } else {
                            throw new IllegalStateException(e);
                        }
                    }
                }
            }
            return FileVisitResult.CONTINUE;
        }
    }
}
