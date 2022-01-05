/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.runtime;

import com.alibaba.fastffi.FFIBuiltinType;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFINameSpace;
import com.alibaba.fastffi.FFINativeLibraryLoader;
import com.alibaba.fastffi.FFIType;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFIVector;
import com.alibaba.fastffi.impl.CXXStdString;
import com.alibaba.fastffi.impl.CXXStdVector;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class FFITypeFactory {

    static class TypeRegistry {
        Map<String, Object> ffiTypeNameToFFIPointerJavaClassName = new ConcurrentHashMap<>();
        Map<String, Object> ffiTypeNameToFFILibraryJavaClassName = new ConcurrentHashMap<>();

        public String toString() {
            return String.format(
                    "{ffiPointers=%s, ffiLibraries=%s}",
                    ffiTypeNameToFFIPointerJavaClassName, ffiTypeNameToFFILibraryJavaClassName);
        }

        public Object getFFIPointer(String ffiTypeName) {
            Object value = this.ffiTypeNameToFFIPointerJavaClassName.get(ffiTypeName);
            if (value == null) {
                throw new IllegalArgumentException("Cannot get FFIPointer for " + ffiTypeName);
            }
            return value;
        }

        public Object getFFILibrary(String ffiTypeName) {
            Object value = this.ffiTypeNameToFFILibraryJavaClassName.get(ffiTypeName);
            if (value == null) {
                throw new IllegalArgumentException("Cannot get FFILibrary for " + ffiTypeName);
            }
            return value;
        }
    }

    static ConcurrentHashMap<ClassLoader, TypeRegistry> loaded = new ConcurrentHashMap<>();

    static {
        loadClassLoader(FFITypeFactory.class.getClassLoader());
    }

    public static void loadClassLoader(ClassLoader classLoader) {
        if (loaded.containsKey(classLoader)) {
            return;
        }
        TypeRegistry typeRegistry = new TypeRegistry();
        try {
            loadFFIProperties(
                    classLoader,
                    "ffi.properties",
                    typeRegistry.ffiTypeNameToFFIPointerJavaClassName);
            loadFFIProperties(
                    classLoader,
                    "ffilibrary.properties",
                    typeRegistry.ffiTypeNameToFFILibraryJavaClassName);
        } finally {
            loaded.put(classLoader, typeRegistry);
        }
    }

    public static void unloadClassLoader(ClassLoader classLoader) {
        loaded.remove(classLoader);
    }

    static void loadFFIProperties(
            ClassLoader classLoader, String proptiesFileName, Map<String, Object> results) {
        try {
            Enumeration<URL> urls = classLoader.getResources(proptiesFileName);
            while (urls.hasMoreElements()) {
                URL propFileURL = urls.nextElement();
                if (propFileURL != null) {
                    try (InputStream inputStream = propFileURL.openStream()) {
                        Properties prop = new Properties();
                        prop.load(inputStream);
                        for (Map.Entry<Object, Object> entry : prop.entrySet()) {
                            if (entry.getKey() == null || entry.getValue() == null) {
                                throw new IllegalStateException(
                                        "Malformed properties file: " + proptiesFileName);
                            }
                            // A generated class name (i.e., the key) is unique
                            // while a foreign type name (i.e., the value) is not unique.
                            // Why here use key as value and value as key here?
                            // This is because we need to query the generate class via the given
                            // key.
                            String stringKey = entry.getValue().toString();
                            String stringValue = entry.getKey().toString();
                            Object check = results.get(stringKey);
                            if (check != null) {
                                if (check instanceof String) {
                                    if (check.equals(stringValue)) {
                                        continue;
                                    } else {
                                        List<String> list = new ArrayList<>(2);
                                        list.add((String) check);
                                        list.add(stringValue);
                                        results.put(stringKey, list);
                                    }
                                } else {
                                    List<String> uncheckedList = (List<String>) check;
                                    uncheckedList.add(stringValue);
                                }
                            } else {
                                results.put(stringKey, stringValue);
                            }
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException(
                                "Cannot load resource from properties file: " + proptiesFileName);
                    }
                } else {
                    System.out.println("No properties file: " + proptiesFileName);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot search for " + proptiesFileName);
        }
    }

    private static Class<?> getFFIPointerImpl(
            Class<?> ffiType, ClassLoader classLoader, String ffiTypeName)
            throws ClassNotFoundException {
        ensureFFIType(ffiType);
        List<String> candidates = getFFIPointerImplClassName(classLoader, ffiTypeName);
        if (candidates.isEmpty()) {
            throw new ClassNotFoundException("No candidates for " + ffiTypeName);
        }
        if (ffiType == null) {
            throw new ClassNotFoundException("FFIType should not be null for " + ffiTypeName);
        }
        for (String javaClass : candidates) {
            Class<?> clazz = getOrLoadClass(classLoader, ffiTypeName, javaClass);
            if (ffiType.isAssignableFrom(clazz)) {
                return clazz;
            }
        }
        throw new ClassNotFoundException(
                "Cannot find a FFIType for " + ffiType + " with FFI type name " + ffiTypeName);
    }

    private static Class<?> getFactoryType(Class<?> ffiImpl) throws ClassNotFoundException {
        FFIForeignType foreignType = ffiImpl.getAnnotation(FFIForeignType.class);
        if (foreignType == null || foreignType.factory() == void.class) {
            throw new ClassNotFoundException("Cannot find a Java factory class for " + ffiImpl);
        }
        return foreignType.factory();
    }

    private static void ensureFFIType(Class<?> ffiType) {
        if (!FFIType.class.isAssignableFrom(ffiType)) {
            throw new IllegalStateException("Type " + ffiType + " is not a valid FFIType.");
        }
    }

    public static class ParameterizedTypeImpl implements ParameterizedType {

        final Type[] typeArguments;
        final Class<?> rawType;

        public ParameterizedTypeImpl(Class<?> rawType, Type[] typeArguments) {
            this.rawType = rawType;
            this.typeArguments = typeArguments;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return typeArguments;
        }

        @Override
        public Type getRawType() {
            return rawType;
        }

        @Override
        public Type getOwnerType() {
            throw new IllegalStateException("Not implemented yet");
        }

        public String toString() {
            return getRawType().getTypeName()
                    + "<"
                    + Arrays.stream(getActualTypeArguments())
                            .map(t -> t.getTypeName())
                            .collect(Collectors.joining(","))
                    + ">";
        }
    }

    public static ParameterizedType makeParameterizedType(Class<?> rawType, Type... arguments) {
        int rawLength = rawType.getTypeParameters().length;
        if (rawLength == 0) {
            throw new IllegalArgumentException("Not a generic type: " + rawType);
        }
        if (rawLength != arguments.length) {
            throw new IllegalArgumentException(
                    "Expected " + rawLength + " type arguments, got " + arguments.length);
        }
        for (Type type : arguments) {
            if (type instanceof Class) {
                Class cls = (Class) type;
                if (cls.isArray() || cls.isPrimitive()) {
                    throw new IllegalArgumentException(
                            "Array and primitive are not supported yet: " + cls);
                }
            } else if (type instanceof ParameterizedType) {
                continue;
            } else {
                throw new IllegalArgumentException("Unsupported type argument: " + type);
            }
        }
        return new ParameterizedTypeImpl(rawType, arguments);
    }

    public static String getFFITypeName(Type ffiType, boolean useDefaultMapping) {
        if (ffiType instanceof Class) {
            return getFFITypeName((Class) ffiType, useDefaultMapping, false);
        }
        if (ffiType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) ffiType;
            String baseType =
                    getFFITypeName(
                            (Class<?>) parameterizedType.getRawType(), useDefaultMapping, true);
            return baseType
                    + "<"
                    + Arrays.stream(parameterizedType.getActualTypeArguments())
                            .map(t -> getFFITypeName(t, useDefaultMapping))
                            .collect(Collectors.joining(","))
                    + ">";
        }
        throw new IllegalArgumentException("Unsupported type: " + ffiType);
    }

    private static String getFFITypeName(Class<?> ffiType) {
        return getFFITypeName(ffiType, false, false);
    }

    private static String getFFITypeName(
            Class<?> ffiType, boolean useDefaultMapping, boolean allowGeneric) {
        if (ffiType.getTypeParameters().length != 0) {
            if (!allowGeneric) {
                throw new IllegalArgumentException(
                        "Type " + ffiType + " has type parameters but generic is not allowed.");
            }
        }
        if (!FFIType.class.isAssignableFrom(ffiType)) {
            if (useDefaultMapping) {
                return getDefaultMapping(ffiType);
            }
            throw new IllegalArgumentException("Type " + ffiType + " is not a FFI type");
        }
        if (FFIBuiltinType.class.isAssignableFrom(ffiType)) {
            if (ffiType.equals(FFIVector.class)) {
                return "std::vector";
            }
            if (ffiType.equals(FFIByteString.class)) {
                return "std::string";
            }
        }
        FFITypeAlias typeAlias = ffiType.getAnnotation(FFITypeAlias.class);
        if (typeAlias == null || typeAlias.value().isEmpty()) {
            throw new IllegalStateException("Type " + ffiType + " has no non-empty FFITypeAlias.");
        }
        FFINameSpace nameSpace = ffiType.getAnnotation(FFINameSpace.class);
        if (nameSpace != null) {
            return nameSpace.value() + "::" + typeAlias.value();
        }
        return typeAlias.value();
    }

    private static String getDefaultMappingBoxed(String typeName, Class<?> ffiType) {
        switch (typeName) {
            case "java.lang.Byte":
                assertEquals(ffiType, Byte.class);
                return "jbyte";
            case "java.lang.Boolean":
                assertEquals(ffiType, Boolean.class);
                return "jboolean";
            case "java.lang.Short":
                assertEquals(ffiType, Short.class);
                return "jshort";
            case "java.lang.Character":
                assertEquals(ffiType, Character.class);
                return "jchar";
            case "java.lang.Integer":
                assertEquals(ffiType, Integer.class);
                return "jint";
            case "java.lang.Float":
                assertEquals(ffiType, Float.class);
                return "jfloat";
            case "java.lang.Long":
                assertEquals(ffiType, Long.class);
                return "jlong";
            case "java.lang.Double":
                assertEquals(ffiType, Double.class);
                return "jdouble";
            default:
                break;
        }
        throw new IllegalArgumentException("Cannot get default mapping for " + ffiType);
    }

    private static String getDefaultMapping(Class<?> ffiType) {
        String typeName = ffiType.getTypeName();
        switch (typeName) {
            case "byte":
                assertEquals(ffiType, byte.class);
                return "jbyte";
            case "boolean":
                assertEquals(ffiType, boolean.class);
                return "jboolean";
            case "short":
                assertEquals(ffiType, short.class);
                return "jshort";
            case "char":
                assertEquals(ffiType, char.class);
                return "jchar";
            case "int":
                assertEquals(ffiType, int.class);
                return "jint";
            case "float":
                assertEquals(ffiType, float.class);
                return "jfloat";
            case "long":
                assertEquals(ffiType, long.class);
                return "jlong";
            case "double":
                assertEquals(ffiType, double.class);
                return "jdouble";
            default:
                break;
        }
        return getDefaultMappingBoxed(typeName, ffiType);
    }

    public static Class<?> getType(Class<?> ffiType, String ffiTypeName)
            throws ClassNotFoundException {
        return getFFIPointerImpl(ffiType, ffiType.getClassLoader(), ffiTypeName);
    }

    /**
     * Specifically for us, load different ffi type with different class loader
     *
     * @param classLoader
     * @param ffiTypeName
     * @return
     * @throws ClassNotFoundException
     */
    public static Class<?> getType(ClassLoader classLoader, String ffiTypeName)
            throws ClassNotFoundException {
        return getFFIPointerImpl(FFIType.class, classLoader, ffiTypeName);
    }

    public static Class<?> getType(String ffiTypeName) throws ClassNotFoundException {
        return getType(FFIType.class, ffiTypeName);
    }

    public static Class<?> getType(Class<?> ffiType) throws ClassNotFoundException {
        return getFFIPointerImpl(ffiType, ffiType.getClassLoader(), getFFITypeName(ffiType));
    }

    public static <T> T getFactory(String ffiTypeName) {
        return getFactory(FFIType.class, ffiTypeName);
    }

    /**
     * Get the factory from the FFIType. The FFIType must not have type parameters, which means it
     * cannot be annotated with CXXTemplate
     *
     * @param ffiType
     * @param <T>
     * @return
     */
    public static <T> T getFactory(Class<?> ffiType) {
        return getFactory(ffiType, getFFITypeName(ffiType));
    }

    public static <T> T getFactory(Class<T> factoryType, Class<?> ffiType) {
        return factoryType.cast(getFactory(ffiType));
    }

    public static <T> T getFactory(Class<?> ffiType, String ffiTypeName) {
        ensureFFIType(ffiType);
        try {
            Class<?> ffiPointerImpl =
                    getFFIPointerImpl(ffiType, ffiType.getClassLoader(), ffiTypeName);
            return getFactoryFromFFIPointerImpl(ffiPointerImpl);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Fail to getFactory for "
                            + ffiTypeName
                            + " via classloader "
                            + ffiType.getClassLoader(),
                    e);
        }
    }

    private static <T> T getFactoryFromFFIPointerImpl(Class<?> ffiImpl) throws Exception {
        Class<?> factoryType = getFactoryType(ffiImpl);
        Field field = factoryType.getDeclaredField("INSTANCE");
        return (T) field.get(null);
    }

    /**
     * Get the FFILibrary with the given library interface and ffi name
     *
     * @param libraryInterface
     * @param ffiLibraryName
     * @param <T>
     * @return
     */
    public static <T> T getLibrary(Class<T> libraryInterface, String ffiLibraryName) {
        try {
            Class<?> libraryType = getLibraryClass(libraryInterface, ffiLibraryName);
            Field field = libraryType.getDeclaredField("INSTANCE");
            return (T) field.get(null);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Fail to get FFILibrary for "
                            + libraryInterface
                            + " with name "
                            + ffiLibraryName,
                    e);
        }
    }

    public static <T> T getLibrary(Class<T> libraryInterface) {
        FFILibrary ffiLibrary = libraryInterface.getAnnotation(FFILibrary.class);
        if (ffiLibrary == null) {
            throw new IllegalArgumentException(
                    "Cannot getLibrary for class "
                            + libraryInterface
                            + " due to missing annotation FFILibrary");
        }
        String ffiTypeId = ffiLibrary.value();
        if (ffiTypeId.isEmpty()) {
            ffiTypeId = ffiLibrary.namespace();
        }
        if (ffiTypeId.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot getLibrary for class "
                            + libraryInterface
                            + " due to empty type registry id.");
        }
        return getLibrary(libraryInterface, ffiLibrary.value());
    }

    static TypeRegistry ensureLoaded(ClassLoader cl) {
        loadClassLoader(cl);
        TypeRegistry registry = loaded.get(cl);
        if (registry == null) {
            throw new IllegalStateException("Class loader " + cl + " cannot be loaded.");
        }
        return registry;
    }

    static Class<?> getOrLoadClass(ClassLoader cl, String ffiTypeName, String javaClassName)
            throws ClassNotFoundException {
        if (javaClassName == null) {
            throw new ClassNotFoundException("Cannot find a Java class for " + ffiTypeName);
        }
        return Class.forName(javaClassName, false, cl);
    }

    /**
     * Every class loader maintains a mapping between ffiTypeName and the Java implementation class
     * name.
     *
     * @param cl
     * @param ffiTypeName
     * @return
     */
    static List<String> getFFIPointerImplClassName(ClassLoader cl, String ffiTypeName) {
        return toStringList(ensureLoaded(cl).getFFIPointer(ffiTypeName));
    }

    static List<String> toStringList(Object o) {
        if (o == null) {
            throw new NullPointerException();
        }
        if (o instanceof String) {
            return Collections.singletonList((String) o);
        }
        return (List<String>) o;
    }

    static List<String> getFFILibraryImplClassName(ClassLoader cl, String ffiTypeName) {
        return toStringList(ensureLoaded(cl).getFFILibrary(ffiTypeName));
    }

    private static Class<?> getLibraryClass(Class<?> libraryClass, String ffiLibraryName)
            throws ClassNotFoundException {
        ClassLoader cl = libraryClass.getClassLoader();
        List<String> candidates = getFFILibraryImplClassName(cl, ffiLibraryName);
        for (String javaClass : candidates) {
            Class<?> clazz = getOrLoadClass(cl, ffiLibraryName, javaClass);
            if (libraryClass.isAssignableFrom(clazz)) {
                return clazz;
            }
        }
        throw new ClassNotFoundException(
                "Cannot find a FFILibrary for "
                        + libraryClass
                        + " with FFI type name "
                        + ffiLibraryName);
    }

    public static String findNativeLibrary(Class<?> clazz, String libraryName) {
        return FFINativeLibraryLoader.findLibrary(clazz, libraryName);
    }

    public static FFIByteString newByteString() {
        return CXXStdString.factory.create();
    }

    public static FFIByteString.Factory getFFIByteStringFactory() {
        return CXXStdString.factory;
    }

    /**
     * As slow as reflection
     *
     * @param elementType
     * @param <E>
     * @return
     */
    public static <E> FFIVector<E> newFFIVector(Type elementType) {
        CXXStdVector.Factory<E> factory = getFFIVectorFactory(elementType);
        return factory.create();
    }

    static void assertEquals(Object o1, Object o2) {
        if (!o1.equals(o2)) {
            throw new IllegalArgumentException("Expected " + o1 + ", get " + o2);
        }
    }

    static String getFFIVectorElementTypeName(Type elementType) {
        return getFFITypeName(elementType, true);
    }

    /**
     * accept primitive types, boxed primitive types, and
     *
     * @param elementType
     * @param <T>
     * @return
     */
    public static <T extends FFIVector.Factory> T getFFIVectorFactory(Type elementType) {
        return getFactory(
                CXXStdVector.class,
                "std::vector<" + getFFIVectorElementTypeName(elementType) + ">");
    }
}
