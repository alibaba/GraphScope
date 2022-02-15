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
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppBaseParser {
    private static Logger logger = LoggerFactory.getLogger(AppBaseParser.class.getName());

    public static void main(String[] args) {
        if (args.length != 1) {
            logger.error("Error: Expected only one class, fully named.");
            return;
        }
        loadClassAndParse(args[0]);
    }

    private static void loadClassAndParse(String className) {
        try {
            Class<?> clz = Class.forName(className);
            Type[] typeParams;
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
            } else {
                String typeParamNames[] = new String[4];
                for (int i = 0; i < 4; ++i) {
                    typeParamNames[i] = typeParams[i].getTypeName();
                }
                logger.info("TypeParams: " + String.join(",", typeParamNames));
                Class<?> ctxType = (Class<?>) typeParams[4];
                logger.info("ContextType:" + javaContextToCppContextName(ctxType));
            }
        } catch (Exception e) {
            logger.info("Exception occurred");
            e.printStackTrace();
        }
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
