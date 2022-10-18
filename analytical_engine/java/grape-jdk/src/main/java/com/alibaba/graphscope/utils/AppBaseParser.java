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

package com.alibaba.graphscope.utils;

import com.alibaba.graphscope.app.DefaultAppBase;
import com.alibaba.graphscope.app.DefaultPropertyAppBase;
import com.alibaba.graphscope.app.ParallelAppBase;
import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.LabeledVertexPropertyContext;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.context.VertexPropertyContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

public class AppBaseParser {
    private static Logger logger = LoggerFactory.getLogger(AppBaseParser.class.getName());
    private static String GIRAPH_APP_ABSTRACT_NAME = "org.apache.giraph.graph.AbstractComputation";
    private static String GIRAPH_APP_BASIC_NAME = "org.apache.giraph.graph.BasicComputation";

    public static void main(String[] args) {
        if (args.length != 1) {
            logger.error("Error: Expected only one class, fully named.");
            return;
        }
        loadClassAndParse(args[0]);
    }

    private static void loadClassAndParse(String className) {
        try {
            // we don't giraphAppBaseClass is defined in giraph-sdk, and we don't want to introduce
            // circular dependency.
            Class<?> giraphDefaultAppBase = Class.forName(GIRAPH_APP_BASIC_NAME);
            logger.info("loaded giraph default app base: " + giraphDefaultAppBase.getName());
            Class<?> clz = Class.forName(className);
            Type[] typeParams;
            // Input class name can be a giraph app, But we can use isAssignableFrom, since it
            // will introduce circular dependency. We judge by using get super class.
            if (tryGiraphClass(clz)) {
                return;
            }
            if (DefaultAppBase.class.isAssignableFrom(clz)) {
                logger.info("DefaultAppBase");
                typeParams = getTypeParams(clz, 5);
            } else if (ParallelAppBase.class.isAssignableFrom(clz)) {
                logger.info("ParallelAppBase");
                typeParams = getTypeParams(clz, 5);
            } else if (DefaultPropertyAppBase.class.isAssignableFrom(clz)) {
                logger.info("DefaultPropertyAppBase");
                typeParams = getTypeParams(clz, 2);
            } else if (ParallelPropertyAppBase.class.isAssignableFrom(clz)) {
                logger.info("ParallelPropertyAppBase");
                typeParams = getTypeParams(clz, 2);
            } else {
                logger.error("No matching app bases");
                return;
            }
            if (typeParams.length == 2) {
                logger.info("TypeParams: " + typeParams[0].getTypeName());
                Class<?> ctxType = (Class<?>) typeParams[1];
                logger.info("ContextType:" + javaContextToCppContextName(ctxType));
                logger.info("VertexData: " + getVertexDataType(ctxType));
            } else {
                String typeParamNames[] = new String[4];
                for (int i = 0; i < 4; ++i) {
                    typeParamNames[i] = typeParams[i].getTypeName();
                }
                logger.info("TypeParams: " + String.join(",", typeParamNames));
                Class<?> ctxType = (Class<?>) typeParams[4];
                logger.info("ContextType:" + javaContextToCppContextName(ctxType));
                logger.info("VertexData: " + getVertexDataType(ctxType));
            }
        } catch (Exception e) {
            logger.info("Exception occurred");
            e.printStackTrace();
        }
    }

    private static String getVertexDataType(Class<?> ctxClass){
        Type[] types = getExtendTypeParams(ctxClass,2);
        return types[1].getTypeName();
    }

    private static boolean tryGiraphClass(Class<?> claz) {
        Class<?> father = claz.getSuperclass();
        if (Objects.isNull(father)) {
            logger.info("Received an interface");
            return false;
        } else if (father.equals(Object.class)) {
            logger.info("super class is object");
            return false;
        }
        Type[] types;
        if (father.getName().equals(GIRAPH_APP_ABSTRACT_NAME)) {
            logger.info("Extend abstract computation");
            types = getExtendTypeParams(claz, 5);
        } else if (father.getName().equals(GIRAPH_APP_BASIC_NAME)) {
            logger.info("Extend basic computation");
            types = getExtendTypeParams(claz, 4);
        } else {
            return false;
        }
        String typeParamNames[] = new String[types.length];
        for (int i = 0; i < types.length; ++i) {
            typeParamNames[i] = types[i].getTypeName();
        }
        logger.info("TypeParams: " + String.join(",", typeParamNames));
        logger.info("ContextType:vertex_data");
        logger.info("VertexData: " + writableToCpp(typeParamNames[1]));
        return true;
    }

    private static String javaContextToCppContextName(Class<?> ctxClass) {
        if (LabeledVertexDataContext.class.isAssignableFrom(ctxClass)) {
            return "labeled_vertex_data";
        } else if (VertexDataContext.class.isAssignableFrom(ctxClass)) {
            return "vertex_data";
        } else if (LabeledVertexPropertyContext.class.isAssignableFrom(ctxClass)) {
            return "labeled_vertex_property";
        } else if (VertexPropertyContext.class.isAssignableFrom(ctxClass)) {
            return "vertex_property";
        }
        return "null";
    }

    private static Type[] getExtendTypeParams(Class<?> klass, int expectedSize) {
        ParameterizedType type = (ParameterizedType) (klass.getGenericSuperclass());
        Type[] classes = type.getActualTypeArguments();
        if (classes.length != expectedSize) {
            logger.error(
                    "Error: Number of params error, expected "
                            + expectedSize
                            + ", actual "
                            + classes.length);
            throw new IllegalStateException("Type params size not match");
        }
        return classes;
    }

    private static Type[] getTypeParams(Class<?> clz, int size) {
        Type type = clz.getGenericInterfaces()[0];
        if (!(type instanceof ParameterizedType)) {
            logger.error("Not a parameterized type" + type.getTypeName());
            throw new IllegalStateException("not a parameterized type" + type.getTypeName());
        }
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Type[] typeParams = parameterizedType.getActualTypeArguments();
        if (typeParams.length != size) {
            logger.error(
                    "Error: Number of params error, expected "
                            + size
                            + ", actual "
                            + typeParams.length);
            throw new IllegalStateException("Type params size not match");
        }
        return typeParams;
    }

    private static String writableToCpp(String typeName){
        if (typeName.contains("DoubleWritable")){
            return "double";
        }
        else if (typeName.contains("IntWritable")){
            return "int32_t";
        }
        else if (typeName.contains("LongWritable")){
            return "int64_t";
        }
        else throw new IllegalStateException("Not recognized writable " + typeName);
    }

    private static Method getMethod(Class<?> clz) {
        Method[] methods = clz.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals("PEval")) {
                return method;
            }
        }
        return null;
    }

    private static Class<?> getFragmentClassFromMethod(Method method) {
        Class<?>[] params = method.getParameterTypes();
        if (params.length != 3) {
            logger.error("Expected 3 parameters for this method: " + method.getName());
            return null;
        }
        return params[0];
    }
}
