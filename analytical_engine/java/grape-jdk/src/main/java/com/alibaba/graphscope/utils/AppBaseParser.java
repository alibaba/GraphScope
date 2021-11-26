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
import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.LabeledVertexPropertyContext;
import com.alibaba.graphscope.context.VertexDataContext;
import com.alibaba.graphscope.context.VertexPropertyContext;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class AppBaseParser {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Error: Expected only one class, fully named.");
            return;
        }
        loadClassAndParse(args[0]);
    }

    private static void loadClassAndParse(String className) {
        try {
            Class<?> clz = Class.forName(className);
            boolean flag = DefaultPropertyAppBase.class.isAssignableFrom(clz);
            if (flag == true) {
                System.out.println("DefaultPropertyApp");
                Class<? extends DefaultPropertyAppBase> clzCasted =
                        (Class<? extends DefaultPropertyAppBase>) clz;
                Type type = clzCasted.getGenericInterfaces()[0];
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    Type[] typeParams = parameterizedType.getActualTypeArguments();
                    if (typeParams.length != 2) {
                        System.out.println(
                                "Error: Number of params error, expected 2, actuval "
                                        + typeParams.length);
                        return;
                    }
                    System.out.println("TypeParams: " + typeParams[0].getTypeName());
                    Class<?> ctxType = (Class<?>) typeParams[1];
                    System.out.println("ContextType:" + javaContextToCppContextName(ctxType));
                    return;
                }
                System.out.println("Error: Not a parameterized type " + type.getTypeName());
                return;
            }

            flag = ParallelPropertyAppBase.class.isAssignableFrom(clz);
            if (flag == true) {
                System.out.println("ParallelPropertyApp");
                Class<? extends ParallelPropertyAppBase> clzCasted =
                        (Class<? extends ParallelPropertyAppBase>) clz;
                Type type = clzCasted.getGenericInterfaces()[0];
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    Type[] typeParams = parameterizedType.getActualTypeArguments();
                    if (typeParams.length != 2) {
                        System.out.println(
                                "Error: Number of params error, expected 2, actuval "
                                        + typeParams.length);
                        return;
                    }
                    System.out.println("TypeParams: " + typeParams[0].getTypeName());
                    Class<?> ctxType = (Class<?>) typeParams[1];
                    System.out.println("ContextType:" + javaContextToCppContextName(ctxType));
                    return;
                }
                System.out.println("Error: Not a parameterized type " + type.getTypeName());
                return;
            }
            // try Projected
            flag = DefaultAppBase.class.isAssignableFrom(clz);
            if (flag == true) {
                System.out.println("DefaultAppBase");
                Class<? extends DefaultAppBase> clzCasted = (Class<? extends DefaultAppBase>) clz;
                Type type = clzCasted.getGenericInterfaces()[0];
                if (type instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) type;
                    Type[] typeParams = parameterizedType.getActualTypeArguments();
                    String[] typeParamNames = new String[4];
                    if (typeParams.length != 5) {
                        System.out.println(
                                "Error: Number of params error, expected 5, actuval "
                                        + typeParams.length);
                        return;
                    }
                    for (int i = 0; i < 4; ++i) {
                        typeParamNames[i] = typeParams[i].getTypeName();
                    }
                    System.out.println("TypeParams: " + String.join(",", typeParamNames));
                    Class<?> ctxType = (Class<?>) typeParams[4];
                    System.out.println("ContextType:" + javaContextToCppContextName(ctxType));
                    return;
                }
                System.out.println("Error: Not a parameterized type " + type.getTypeName());
                return;
            }
            System.out.println("Unrecognizable class Name");
        } catch (Exception e) {
            System.out.println("Exception occurred");
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
            System.err.println("Expected 3 parameters for this method: " + method.getName());
            return null;
        }
        return params[0];
    }
}
