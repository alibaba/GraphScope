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

package com.alibaba.graphscope.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

/**
 * GraphScope Class Loader contains several static functions which will be used by GraphScope
 * grape_engine at runtime, to initiating java runtime environment. When java app(packed in jar) is
 * submitted to grape engine, a jvm will be initiated via JNI interface pointer if jvm has not been
 * created is this process. Rather than directly load java classes via JNI interface pointer, we
 * utilize a URLClassLoader for each jar to do this. The benefit of this solution is the ioslated
 * jvm environment provided by class loaders, which can avoid possible class conflicts.
 */
public class GraphScopeClassLoader {
    private static Logger logger = LoggerFactory.getLogger(GraphScopeClassLoader.class);
    private static String FFI_TYPE_FACTORY_CLASS = "com.alibaba.graphscope.runtime.FFITypeFactory";

    static {
        try {
            System.loadLibrary("grape-jni");
            logger.info("loaded jni lib");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a new URLClassLoaders with given classPaths. The classPath shall be in form {@code
     * /path/to/A:B.jar}.
     *
     * @param classPath a string will be interpreted as java class path.
     * @return new classloader with specified classPath.
     * @throws IllegalAccessException if ClassScope can not get loaded libraries.
     */
    public static URLClassLoader newGraphScopeClassLoader(String classPath)
            throws IllegalAccessException {
        String[] libraries = ClassScope.getLoadedLibraries(ClassLoader.getSystemClassLoader());
        logger.debug("Loaded lib: " + String.join(" ", libraries));
        URLClassLoader urlClassLoader = new URLClassLoader(classPath2URLArray(classPath));
        logger.debug(
                "URLClassLoader loaded lib: "
                        + String.join(",", ClassScope.getLoadedLibraries(urlClassLoader)));
        logger.info(
                "Class Loader "
                        + urlClassLoader
                        + " serach path: "
                        + urlsToString(urlClassLoader.getURLs()));
        return urlClassLoader;
    }

    /**
     * Return a default URLClassLoader with no classPath.
     *
     * @return the default class loader.
     * @throws IllegalAccessException if ClassScope can not get loaded libraries.
     */
    public static URLClassLoader newGraphScopeClassLoader() throws IllegalAccessException {
        String[] libraries = ClassScope.getLoadedLibraries(ClassLoader.getSystemClassLoader());
        logger.debug("Loaded lib: " + String.join(" ", libraries));
        // CAUTION: add '.' to avoid empty url.
        return new URLClassLoader(
                classPath2URLArray("."), Thread.currentThread().getContextClassLoader());
    }

    /**
     * Load the specified class with one URLClassLoader, return a new instance for this class. The
     * class name could be fully-specified or dash-separated.
     *
     * @param classLoader
     * @param className   a/b/c/ or a.b.c
     * @return a instance for loaded class.
     * @throws ClassNotFoundException if class can not be found in current path.
     * @throws InstantiationException if error in creating new instance.
     * @throws IllegalAccessException if error in creating new instance.
     */
    public static Object loadAndCreate(
            URLClassLoader classLoader, String className, String serialPath)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
                    InvocationTargetException, NoSuchMethodException {
        logger.info("Load and create: " + formatting(className));
        Class<?> clz = classLoader.loadClass(formatting(className));
        return clz.newInstance();
    }

    /**
     * This function wrap one C++ object into a java object, namely a FFIPointer.
     *
     * @param classLoader The class loader with used to load java classes.
     * @param foreignName The foreign name for C++ object,shall be fully specified.
     * @param address     The address for C++ object.
     * @return a FFIPointer wrapper.
     * @throws ClassNotFoundException    if class can not be found in current path.
     * @throws NoSuchMethodException     if method for ffi type factory can not be found.
     * @throws InvocationTargetException if error in invoke the specific method.
     * @throws IllegalAccessException    if error in invoke the specific method.
     * @throws InstantiationException    if error in creating new instance.
     */
    public static Object CreateFFIPointer(
            URLClassLoader classLoader, String foreignName, long address)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
                    IllegalAccessException, InstantiationException {
        Class<?> javaClass = null;
        // For FFIIntVector/ FFIByteVector, we use we optimized class.
        if (foreignName.equals("std::vector<char>")) {
            javaClass = classLoader.loadClass("com.alibaba.graphscope.stdcxx.FFIByteVector");
        } else if (foreignName.equals("std::vector<std::vector<char>>")) {
            javaClass = classLoader.loadClass("com.alibaba.graphscope.stdcxx.FFIByteVecVector");
        } else if (foreignName.equals("std::vector<int>")) {
            javaClass = classLoader.loadClass("com.alibaba.graphscope.stdcxx.FFIIntVector");
        } else if (foreignName.equals("std::vector<std::vector<int>>")) {
            javaClass = classLoader.loadClass("com.alibaba.graphscope.stdcxx.FFIIntVecVector");
        } else {
            javaClass = loadFFIClassFromTypeFactory(classLoader, foreignName, address);
        }
        if (Objects.isNull(javaClass)) {
            throw new IllegalArgumentException("Get ffi java class null");
        }

        // The class loaded by FFITypeFactor's classLoader can not be directly used
        // by us. We load again with our class loader.
        //        Class<?> javaClass = classLoader.loadClass(ffiJavaClass.getName());
        if (Objects.nonNull(javaClass)) {
            Constructor[] constructors = javaClass.getDeclaredConstructors();
            for (Constructor constructor : constructors) {
                if (constructor.getParameterCount() == 1
                        && constructor.getParameterTypes()[0].getName().equals("long")) {
                    Object obj = constructor.newInstance(address);
                    return obj;
                }
            }
            logger.info("No Suitable constructors found.");
        }
        logger.error("Loaded null class.");
        return null;
    }

    /**
     * We now accept two kind of className, a/b/c or a.b.c are both ok.
     *
     * @param classLoader url class loader to utilized.
     * @param className   full name for java class.
     * @return loaded class.
     * @throws ClassNotFoundException if target class can not be found in current path.
     */
    public static Class<?> loadClass(URLClassLoader classLoader, String className)
            throws ClassNotFoundException {
        // logger.info("Loading class " + className);
        return classLoader.loadClass(formatting(className));
    }

    /**
     * A special case for loadClass, i.e. loading the communicator class.
     *
     * @param classLoader url class loader to utilized.
     * @return loaded class.
     * @throws ClassNotFoundException if target class can not be found in current path.
     */
    public static Class<?> loadCommunicatorClass(URLClassLoader classLoader)
            throws ClassNotFoundException {
        return loadClass(classLoader, "com.alibaba.graphscope.communication.Communicator");
    }

    private static String formatting(String className) {
        if (!className.contains("/")) {
            return className;
        }
        return className.replace("/", ".");
    }

    private static URL[] classPath2URLArray(String classPath) {
        if (Objects.isNull(classPath) || classPath.length() == 0) {
            logger.error("Empty class Path!");
            return new URL[] {};
        }
        String[] splited = classPath.split(":");
        logger.debug("Splited class path: " + String.join(",", splited));
        List<URL> res =
                Arrays.stream(splited)
                        .map(File::new)
                        .map(
                                file -> {
                                    try {
                                        return file.toURL();
                                    } catch (MalformedURLException e) {
                                        e.printStackTrace();
                                    }
                                    return null;
                                })
                        .collect(Collectors.toList());
        logger.debug(
                "Extracted URL: "
                        + String.join(
                                ":", res.stream().map(URL::toString).collect(Collectors.toList())));
        URL[] ret = new URL[splited.length];
        for (int i = 0; i < splited.length; ++i) {
            ret[i] = res.get(i);
        }
        return ret;
    }

    /**
     * Get the actual argument a child class has to implement a generic interface.
     *
     * @param baseClass  baseclass
     * @param childClass child class
     * @param <T>        type to evaluation
     * @return
     */
    public static <T> Class<?>[] getTypeArgumentFromInterface(
            Class<T> baseClass, Class<? extends T> childClass) {
        Type type = childClass.getGenericInterfaces()[0];
        Class<?>[] classes;
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type[] typeParams = parameterizedType.getActualTypeArguments();
            classes = new Class<?>[typeParams.length];
            for (int i = 0; i < typeParams.length; ++i) {
                classes[i] = (Class<?>) typeParams[i];
            }
            return classes;
        } else {
            throw new IllegalStateException("Not a parameterized type");
        }
    }

    private static Class<?> loadFFIClassFromTypeFactory(
            URLClassLoader classLoader, String foreignName, long address)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
                    ClassNotFoundException {
        // FFITypeFactor class need to be ensure loaded in current classLoader,
        // don't make it static.
        logger.debug(
                "class loader path: "
                        + (Arrays.stream(classLoader.getURLs())
                                .map(URL::toString)
                                .collect(Collectors.joining())));
        Class<?> ffiTypeFactoryClass = classLoader.loadClass(FFI_TYPE_FACTORY_CLASS);
        logger.debug(
                "Creating FFIPointer, typename ["
                        + foreignName
                        + "], address ["
                        + address
                        + "]"
                        + ", ffi type factory ["
                        + ffiTypeFactoryClass
                        + "], loaded by "
                        + ffiTypeFactoryClass.getClassLoader());
        // a new classLoader contains new class path, we load the ffi.properties
        // here.
        Method loadClassLoaderMethod =
                ffiTypeFactoryClass.getDeclaredMethod("loadClassLoader", ClassLoader.class);
        loadClassLoaderMethod.invoke(null, classLoader);

        // First load class by FFITypeFactor
        Method getTypeMethod =
                ffiTypeFactoryClass.getDeclaredMethod("getType", ClassLoader.class, String.class);
        return (Class<?>) getTypeMethod.invoke(null, classLoader, foreignName);
    }

    private static String urlsToString(URL[] urls) {
        StringBuilder sb = new StringBuilder();
        for (URL url : urls) {
            sb.append(url);
            sb.append(",");
        }
        return sb.toString();
    }

    private static class ClassScope {
        private static java.lang.reflect.Field LIBRARIES = null;

        static {
            try {
                LIBRARIES = ClassLoader.class.getDeclaredField("loadedLibraryNames");
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            }
            LIBRARIES.setAccessible(true);
        }

        /**
         * Get the libraries already loaded in one classLoader. Note that one lib can not be loaded
         * twice via the same class loader.
         *
         * @param loader
         * @return
         * @throws IllegalAccessException
         */
        public static String[] getLoadedLibraries(final ClassLoader loader)
                throws IllegalAccessException {
            if (getVersion() == 8) {
                final Vector<String> libraries = (Vector<String>) LIBRARIES.get(loader);
                return libraries.toArray(new String[] {});
            } else if (getVersion() >= 11) {
                final Set<String> libraries = (HashSet<String>) LIBRARIES.get(loader);
                return libraries.toArray(new String[] {});
            } else {
                throw new IllegalStateException("Not supported version" + getVersion());
            }
        }
    }

    private static int getVersion() {
        String version = System.getProperty("java.version");
        if (version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if (dot != -1) {
                version = version.substring(0, dot);
            }
        }
        return Integer.parseInt(version);
    }
}
