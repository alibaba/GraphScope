/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.utils;

import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import java.lang.reflect.Modifier;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.mapreduce.Mapper;
import org.jodah.typetools.TypeResolver;

/**
 * Helper methods to get type arguments to generic classes.  Courtesy of
 * Ian Robertson (overstock.com).  Make sure to use with abstract
 * generic classes, not interfaces.
 */
public class ReflectionUtils {

    /**
     * Do not instantiate.
     */
    private ReflectionUtils() {
    }

    /**
     * Get package path to the object given. Used with resources.
     *
     * @param object the Object to check
     * @return Path to package of object
     */
    public static String getPackagePath(Object object) {
        return getPackagePath(object.getClass());
    }

    /**
     * Get package path to the class given. Used with resources.
     *
     * @param klass Class to check
     * @return Path to package of class
     */
    public static String getPackagePath(Class klass) {
        return klass.getPackage().getName().replaceAll("\\.", "/");
    }

    /**
     * Get the actual type arguments a child class has used to extend a
     * generic base class.
     *
     * @param <T> Type to evaluate.
     * @param baseClass the base class
     * @param childClass the child class
     * @return a list of the raw classes for the actual type arguments.
     */
//    public static <T> Class<?>[] getTypeArguments(
//        Class<T> baseClass, Class<? extends T> childClass) {
//        return TypeResolver.resolveArguments(childClass, baseClass);
//    }

    /**
     * Instantiate a class, wrap exceptions
     *
     * @param theClass Class to instantiate
     * @param <T>      Type to instantiate
     * @return Newly instantiated object
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theClass) {
        try {
            return theClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalStateException(
                "newInstance: Couldn't instantiate " + theClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                "newInstance: Illegal access " + theClass.getName(), e);
        }
    }

    /**
     * Instantiate classes that are ImmutableClassesGiraphConfigurable
     *
     * @param theClass      Class to instantiate
     * @param configuration Giraph configuration, may be null
     * @param <T>           Type to instantiate
     * @return Newly instantiated object with configuration set if possible
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(
        Class<T> theClass,
        ImmutableClassesGiraphConfiguration configuration) {
        T result;
        try {
            result = theClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalStateException(
                "newInstance: Couldn't instantiate " + theClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                "newInstance: Illegal access " + theClass.getName(), e);
        }
//        ConfigurationUtils.configureIfPossible(result, configuration);
        return result;
    }

    /**
     * Instantiate classes that are ImmutableClassesGiraphConfigurable,
     * and optionally set context on them if they are ContextSettable
     *
     * @param theClass Class to instantiate
     * @param configuration Giraph configuration, may be null
     * @param context Mapper context
     * @param <T> Type to instantiate
     * @return Newly instantiated object with configuration and context set if
     * possible
     */
//    public static <T> T newInstance(
//        Class<T> theClass,
//        ImmutableClassesGiraphConfiguration configuration,
//        Mapper<?, ?, ?, ?>.Context context) {
//        T result = newInstance(theClass, configuration);
//        if (result instanceof ContextSettable) {
//            ((ContextSettable) result).setContext(context);
//        }
//        return result;
//    }

    /**
     * Verify that found type matches the expected type. If types don't match an
     * {@link IllegalStateException} will be thrown.
     *
     * @param concreteChild Concrete child type
     * @param parent Parent type
     * @param typeDesc String description of the type (for exception description)
     * @param mainClass Class in which the actual type was found (for exception
     *                  description)
     */
//    public static void verifyTypes(Class<?> concreteChild, Class<?> parent,
//        String typeDesc, Class<?> mainClass) {
//        // unknown means object
//        if (parent == TypeResolver.Unknown.class) {
//            parent = Object.class;
//        }
//
//        verifyConcrete(concreteChild, typeDesc);
//
//        if (!parent.isAssignableFrom(concreteChild)) {
//            throw new IllegalStateException("verifyTypes: " + typeDesc + " types " +
//                "don't match, in " + mainClass.getName() + " " + concreteChild +
//                " expected, but " + parent + " found");
//        }
//    }

    /**
     * Verify that given type is a concrete type that can be instantiated.
     *
     * @param concrete type to check
     * @param typeDesc String description of the type (for exception description)
     */
    public static void verifyConcrete(
        Class<?> concrete, String typeDesc) {
        if (concrete.isInterface()) {
            throw new IllegalStateException("verifyTypes: " +
                "Type " + typeDesc + " must be concrete class " + concrete);
        }
        if (Modifier.isAbstract(concrete.getModifiers())) {
            throw new IllegalStateException("verifyTypes: " +
                "Type " + typeDesc + "can't be abstract class" + concrete);
        }
    }

    /**
     * Get the actual type arguments a child class has used to extend a generic base class.
     *
     * @param <T>        Type to evaluate.
     * @param baseClass  the base class
     * @param childClass the child class
     * @return a list of the raw classes for the actual type arguments.
     */
    public static <T> Class<?>[] getTypeArguments(
        Class<T> baseClass, Class<? extends T> childClass) {
        return TypeResolver.resolveArguments(childClass, baseClass);
    }

    /**
     * Get the actual argument a child class has to implement a generic interface.
     * @param baseClass baseclass
     * @param childClass child class
     * @param <T> type to evaluation
     * @return
     */
    public static <T> Class<?>[] getTypeArgumentFromInterface(Class<T> baseClass,
        Class<? extends T> childClass) {
        Type type = childClass.getGenericInterfaces()[0];
        Class<?> []classes;
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type[] typeParams = parameterizedType.getActualTypeArguments();
            classes = new Class<?>[typeParams.length];
            for (int i =0; i < typeParams.length; ++i){
                classes[i] = (Class<?>) typeParams[i];
            }
            return classes;
        } else {
            throw new IllegalStateException("Not a parameterized type");
        }
    }

    /**
     * Verify that found type matches the expected type. If types don't match an
     * {@link IllegalStateException} will be thrown.
     *
     * @param concreteChild Concrete child type
     * @param parent Parent type
     * @param typeDesc String description of the type (for exception description)
     * @param mainClass Class in which the actual type was found (for exception
     *                  description)
     */
    public static void verifyTypes(Class<?> concreteChild, Class<?> parent,
        String typeDesc, Class<?> mainClass) {
        // unknown means object
        if (parent == TypeResolver.Unknown.class) {
            parent = Object.class;
        }

        verifyConcrete(concreteChild, typeDesc);

        if (!parent.isAssignableFrom(concreteChild)) {
            throw new IllegalStateException("verifyTypes: " + typeDesc + " types " +
                "don't match, in " + mainClass.getName() + " " + concreteChild +
                " expected, but " + parent + " found");
        }
    }
}
