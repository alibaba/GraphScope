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
import com.alibaba.graphscope.app.DefaultContextBase;
import com.alibaba.graphscope.app.DefaultPropertyAppBase;
import com.alibaba.graphscope.app.ParallelPropertyAppBase;
import com.alibaba.graphscope.context.LabeledVertexDataContext;
import com.alibaba.graphscope.context.PropertyDefaultContextBase;
import com.alibaba.graphscope.context.VertexDataContext;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class AppContextGetter {
    /**
     * Get the type parameter for the implemented interface
     *
     * @param clz
     * @param index
     * @return
     */
    private static Class<?> getInterfaceTemplateType(Class<?> clz, int index) {
        Type[] genericType = clz.getGenericInterfaces();
        // System.out.println(genericType[0].getTypeName());
        if (!(genericType[0] instanceof ParameterizedType)) {
            System.err.println("not parameterize type");
            return null;
        }
        Type[] typeParams = ((ParameterizedType) genericType[0]).getActualTypeArguments();
        if (index >= typeParams.length || index < 0) {
            System.err.println("only " + typeParams.length + " params , out of index");
            return null;
        }
        // System.out.println(typeParams[index].getTypeName());
        return (Class<?>) typeParams[index];
    }

    /**
     * Get the type parameter at index for the extended class
     *
     * @param clz
     * @param index
     * @return
     */
    private static Class<?> getBaseClassTemplateType(Class<?> clz, int index) {
        // Type[] genericType = clz.getGenericInterfaces();
        Type genericType = clz.getGenericSuperclass();
        // System.out.println(genericType[0].getTypeName());
        if (!(genericType instanceof ParameterizedType)) {
            System.err.println("not parameterize type");
            return null;
        }
        Type[] typeParams = ((ParameterizedType) genericType).getActualTypeArguments();
        if (index >= typeParams.length || index < 0) {
            System.err.println("only " + typeParams.length + " params , out of index");
            return null;
        }
        // System.out.println(typeParams[index].getTypeName());
        return (Class<?>) typeParams[index];
    }

    /**
     * For PropertyDefaultApp, the context should be the 2rd, i.e. index of 1.
     *
     * @param appClass user-defined app class object.
     * @return the base class name.
     */
    public static String getPropertyDefaultContextName(
            Class<? extends DefaultPropertyAppBase> appClass) {
        Class<? extends PropertyDefaultContextBase> clz =
                (Class<? extends PropertyDefaultContextBase>) getInterfaceTemplateType(appClass, 1);
        return clz.getName();
    }

    /**
     * For ProjectedDefaultApp, the context should be the 2rd, i.e. index of 1.
     *
     * @param appClass user-defined app class object.
     * @return the base class name.
     */
    public static String getDefaultContextName(Class<? extends DefaultAppBase> appClass) {
        Class<? extends DefaultContextBase> clz =
                (Class<? extends DefaultContextBase>) getInterfaceTemplateType(appClass, 4);
        return clz.getName();
    }

    /**
     * For parallel property app ,the index of context type in template is 1.
     *
     * @param appClass user-defined app class object.
     * @return the corrsponding class name.
     */
    public static String getParallelPropertyContextName(
            Class<? extends ParallelPropertyAppBase> appClass) {
        Class<? extends PropertyDefaultContextBase> clz =
                (Class<? extends PropertyDefaultContextBase>) getInterfaceTemplateType(appClass, 1);
        return clz.getName();
    }

    public static String getContextName(Object obj) {
        System.out.println("obj class " + obj.getClass().getName());
        if (obj instanceof DefaultPropertyAppBase) {
            return getPropertyDefaultContextName(
                    (Class<? extends DefaultPropertyAppBase>) obj.getClass());
        }
        System.out.println(
                "obj class"
                        + obj.getClass().getName()
                        + " is not instance of DefaultPropertyAppBase.");
        if (obj instanceof DefaultAppBase) {
            return getDefaultContextName((Class<? extends DefaultAppBase>) obj.getClass());
        }
        System.out.println(
                "obj class"
                        + obj.getClass().getName()
                        + " is not instance of ProjectedDefaultAppBase.");

        if (obj instanceof ParallelPropertyAppBase) {
            return getParallelPropertyContextName(
                    (Class<? extends ParallelPropertyAppBase>) obj.getClass());
        }
        System.out.println(
                "obj class"
                        + obj.getClass().getName()
                        + " is not instance of ParallelPropertyAppBase.");
        return null;
    }

    public static String getLabeledVertexDataContextDataType(LabeledVertexDataContext ctxObj) {
        Class<? extends LabeledVertexDataContext> ctxClass = ctxObj.getClass();
        Class<?> ret = getBaseClassTemplateType(ctxClass, 1);
        if (ret.getName() == "java.lang.Double") {
            return "double";
        } else if (ret.getName() == "java.lang.Integer") {
            return "uint32_t";
        } else if (ret.getName() == "java.lang.Long") {
            return "uint64_t";
        }
        return null;
    }

    public static String getVertexDataContextDataType(VertexDataContext ctxObj) {
        Class<? extends VertexDataContext> ctxClass = ctxObj.getClass();
        Class<?> ret = getBaseClassTemplateType(ctxClass, 1);
        if (ret.getName() == "java.lang.Double") {
            return "double";
        } else if (ret.getName() == "java.lang.Integer") {
            return "int32_t";
        } else if (ret.getName() == "java.lang.Long") {
            return "int64_t";
        }
        return null;
    }
}
