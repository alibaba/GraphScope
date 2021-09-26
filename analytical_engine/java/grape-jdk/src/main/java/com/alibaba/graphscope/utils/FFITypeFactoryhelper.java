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

import static com.alibaba.graphscope.utils.CppClassName.GRAPE_MESSAGE_IN_BUFFER;
import static com.alibaba.graphscope.utils.CppClassName.GS_PRIMITIVE_MESSAGE;
import static com.alibaba.graphscope.utils.CppClassName.GS_VERTEX_ARRAY;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastffi.FFIVector;
import com.alibaba.fastffi.impl.CXXStdVector;
import com.alibaba.graphscope.ds.DenseVertexSet;
import com.alibaba.graphscope.ds.EmptyType;
import com.alibaba.graphscope.ds.GSVertexArray;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.ds.VertexArray;
import com.alibaba.graphscope.ds.VertexRange;
import com.alibaba.graphscope.parallel.MessageInBuffer;
import com.alibaba.graphscope.parallel.message.DoubleMsg;
import com.alibaba.graphscope.parallel.message.LongMsg;
import com.alibaba.graphscope.parallel.message.PrimitiveMessage;
import com.alibaba.graphscope.stdcxx.StdString;
import com.alibaba.graphscope.stdcxx.StdString.Factory;
import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FFITypeFactoryhelper {
    private static Logger logger = LoggerFactory.getLogger(FFITypeFactoryhelper.class.getName());
    private static volatile Factory stdStrFactory;
    private static volatile Vertex.Factory vertexFactory;
    private static volatile MessageInBuffer.Factory javaMsgInBufFactory;
    private static volatile EmptyType.Factory emptyTypeFactory;
    private static volatile VertexRange.Factory vertexRangeLongFactory;
    private static volatile DenseVertexSet.Factory denseVertexSetFactory;

    private static volatile HashMap<String, VertexArray.Factory> vertexArrayFactoryMap =
            new HashMap<>();
    private static volatile HashMap<String, GSVertexArray.Factory> gsVertexArrayFactoryMap =
            new HashMap<>();
    private static volatile HashMap<String, FFIVector.Factory> ffiVectorFactoryMap =
            new HashMap<>();
    private static volatile HashMap<String, PrimitiveMessage.Factory> primitiveMsgFactoryMap =
            new HashMap<>();

    public static String javaType2CppType(Class<?> clz) {
        if (clz.getName() == Long.class.getName()) {
            // signed returned
            return "int64_t";
        } else if (clz.getName() == Integer.class.getName()) {
            return "uint32_t";
        } else if (clz.getName() == Double.class.getName()) {
            return "double";
        } else {
            System.err.println("Must be one of long, double, integer");
            return "null";
        }
    }

    public static Factory getStdStringFactory() {
        if (stdStrFactory == null) {
            synchronized (StdString.class) {
                if (stdStrFactory == null) {
                    stdStrFactory = StdString.factory;
                }
            }
        }
        return stdStrFactory;
    }

    public static Vertex.Factory getVertexLongFactory() {
        if (vertexFactory == null) {
            synchronized (Vertex.Factory.class) {
                if (vertexFactory == null) {
                    vertexFactory = FFITypeFactory.getFactory("grape::Vertex<uint64_t>");
                }
            }
        }
        return vertexFactory;
    }

    public static VertexRange.Factory getVertexRangeLongFactory() {
        if (vertexRangeLongFactory == null) {
            synchronized (VertexRange.Factory.class) {
                if (vertexRangeLongFactory == null) {
                    vertexRangeLongFactory =
                            FFITypeFactory.getFactory("grape::VertexRange<uint64_t>");
                }
            }
        }
        return vertexRangeLongFactory;
    }

    public static VertexArray.Factory getVertexArrayFactory(String foreignTypeName) {
        if (!vertexArrayFactoryMap.containsKey(foreignTypeName)) {
            synchronized (vertexArrayFactoryMap) {
                if (!vertexArrayFactoryMap.containsKey(foreignTypeName)) {
                    vertexArrayFactoryMap.put(
                            foreignTypeName,
                            FFITypeFactory.getFactory(VertexArray.class, foreignTypeName));
                }
            }
        }
        return vertexArrayFactoryMap.get(foreignTypeName);
    }

    public static GSVertexArray.Factory getGSVertexArrayFactory(String foreignTypeName) {
        if (!gsVertexArrayFactoryMap.containsKey(foreignTypeName)) {
            synchronized (gsVertexArrayFactoryMap) {
                if (!gsVertexArrayFactoryMap.containsKey(foreignTypeName)) {
                    gsVertexArrayFactoryMap.put(
                            foreignTypeName,
                            FFITypeFactory.getFactory(GSVertexArray.class, foreignTypeName));
                }
            }
        }
        return gsVertexArrayFactoryMap.get(foreignTypeName);
    }

    public static DenseVertexSet.Factory getDenseVertexSetFactory() {
        if (denseVertexSetFactory == null) {
            synchronized (DenseVertexSet.Factory.class) {
                if (denseVertexSetFactory == null) {
                    denseVertexSetFactory =
                            FFITypeFactory.getFactory(
                                    DenseVertexSet.class, "grape::DenseVertexSet<uint64_t>");
                }
            }
        }
        return denseVertexSetFactory;
    }

    /**
     * get the ffiVectorFactor which can produce std::vector, here foreignType can be netsted
     *
     * @param foreignTypeName foreign name (cpp name, full-qualified)
     * @return Factory instance.
     */
    public static FFIVector.Factory getFFIVectorFactory(String foreignTypeName) {
        if (!ffiVectorFactoryMap.containsKey(foreignTypeName)) {
            synchronized (ffiVectorFactoryMap) {
                if (!ffiVectorFactoryMap.containsKey(foreignTypeName)) {
                    ffiVectorFactoryMap.put(
                            foreignTypeName,
                            FFITypeFactory.getFactory(CXXStdVector.class, foreignTypeName));
                }
            }
        }
        return ffiVectorFactoryMap.get(foreignTypeName);
    }

    public static Vertex<Long> newVertexLong() {
        return getVertexLongFactory().create();
    }

    /**
     * This is the same as DoubleMsg.factory.create();
     *
     * @return created instance
     */
    public static PrimitiveMessage<Double> newDoublePrimitiveMsg() {
        String templateStr = GS_PRIMITIVE_MESSAGE + "<double>";
        return getPrimitivemessageFactory(templateStr).create();
    }

    public static PrimitiveMessage<Long> newLongPrimitiveMsg() {
        String templateStr = GS_PRIMITIVE_MESSAGE + "<int64_t>";
        return getPrimitivemessageFactory(templateStr).create();
    }

    /**
     * Create the template msg instance.
     *
     * @param clz element class instace.
     * @param <T> element type
     * @return created instance.
     */
    public static <T> PrimitiveMessage<T> newPrimitiveMsg(Class<T> clz) {
        String templateStr = GS_PRIMITIVE_MESSAGE;
        if (clz.getName().equals(Double.class.getName())) {
            templateStr += "<double>";
        } else if (clz.getName().equals(Long.class.getName())) {
            templateStr += "<int64_t>";
        } else {
            templateStr += FFITypeFactory.getFFITypeName(clz, true);
        }
        return getPrimitivemessageFactory(templateStr).create();
    }

    public static PrimitiveMessage.Factory getPrimitivemessageFactory(String templateStr) {
        if (!primitiveMsgFactoryMap.containsKey(templateStr)) {
            synchronized (primitiveMsgFactoryMap) {
                if (!primitiveMsgFactoryMap.containsKey(templateStr)) {
                    primitiveMsgFactoryMap.put(
                            templateStr,
                            FFITypeFactory.getFactory(PrimitiveMessage.class, templateStr));
                }
            }
        }
        return primitiveMsgFactoryMap.get(templateStr);
    }

    public static VertexRange<Long> newVertexRangeLong() {
        return getVertexRangeLongFactory().create();
    }

    public static <T> VertexArray<T, Long> newVertexArray(Class<T> clz) {
        // NOTE: in this way the returned FFIType name is jni type like jint, jdouble
        String tmp =
                "grape::VertexArray<" + FFITypeFactory.getFFITypeName(clz, true) + ",uint64_t>";
        return getVertexArrayFactory(tmp).create();
    }

    public static <T> GSVertexArray<T> newGSVertexArray(Class<T> clz) {
        String tmp = GS_VERTEX_ARRAY + "<" + javaType2CppType(clz) + ">";
        return getGSVertexArrayFactory(tmp).create();
    }

    /**
     * In case user want to create a nested std::vector instance. the foreign name translation
     * relies on ffi method.
     *
     * @param clz outer class's Class object.
     * @param types inner classes class objects.
     * @param <T> outer class T.
     * @return created FFIVector
     */
    public static <T> FFIVector newComplicateFFIVector(Class<T> clz, Class<?>... types) {
        String[] foriegnNames = new String[types.length];
        for (int i = 0; i < types.length; ++i) {
            foriegnNames[i] = FFITypeFactory.getFFITypeName(types[i], true);
        }
        String nestedForeignName =
                makeParameterize(FFITypeFactory.getFFITypeName(clz, true), foriegnNames);
        return getFFIVectorFactory(nestedForeignName).create();
    }

    public static DenseVertexSet<Long> newDenseVertexSet() {
        return getDenseVertexSetFactory().create();
    }

    public static EmptyType.Factory getEmptyTypeFactory() {
        if (emptyTypeFactory == null) {
            synchronized (EmptyType.Factory.class) {
                if (emptyTypeFactory == null) {
                    emptyTypeFactory = FFITypeFactory.getFactory("grape::EmptyType");
                }
            }
        }
        return emptyTypeFactory;
    }

    public static EmptyType createEmptyType() {
        return getEmptyTypeFactory().create();
    }

    public static MessageInBuffer.Factory newMessageInBuffer() {
        if (javaMsgInBufFactory == null) {
            synchronized (MessageInBuffer.Factory.class) {
                if (javaMsgInBufFactory == null) {
                    javaMsgInBufFactory =
                            (MessageInBuffer.Factory)
                                    FFITypeFactory.getFactory(GRAPE_MESSAGE_IN_BUFFER);
                }
            }
        }
        return javaMsgInBufFactory;
    }

    public static String makeParameterize(String base, String... fields) {
        if (fields.length == 0) {
            return base;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(base);
        sb.append("<");
        sb.append(String.join(",", fields));
        sb.append(">");
        return sb.toString();
    }

    public static String[] getTypeParams(Class<?> clz, int expectedNum) {
        // TypeVariable[] typeVariables = (TypeVariable[]) clz.getTypeParameters();
        Type clzz = (Type) clz;
        System.out.println(clzz.getTypeName());
        if (clzz instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) clzz;
            Type[] types = parameterizedType.getActualTypeArguments();
            if (types.length != expectedNum) {
                logger.error(
                        "Expected param number not consistent with actual:"
                                + types.length
                                + " , "
                                + expectedNum);
            }
            String[] res = new String[expectedNum];
            for (int i = 0; i < expectedNum; ++i) {
                res[i] = types[i].getTypeName();
            }
            return res;
        }
        logger.error("Not a parameterized type");
        return null;
    }

    public static LongMsg newLongMsg() {
        return LongMsg.factory.create();
    }

    public static LongMsg newLongMsg(long value) {
        return LongMsg.factory.create(value);
    }

    public static DoubleMsg newDoubleMsg() {
        return DoubleMsg.factory.create();
    }

    public static DoubleMsg newDoubleMsg(double value) {
        return DoubleMsg.factory.create(value);
    }

    /**
     * For Any ffi-gened class, we can get the typealias via annotation
     *
     * @param ffiPointer Java class generated by ffi.
     * @return foreignName
     */
    public static String getForeignName(FFIPointer ffiPointer) {
        Class<?> clz = ffiPointer.getClass();
        Annotation[] annotations = clz.getDeclaredAnnotations();
        for (Annotation annotation : annotations) {
            logger.info(
                    "Annotation: "
                            + annotation.toString()
                            + ", "
                            + annotation.annotationType().getName()
                            + ","
                            + clz.getAnnotation(annotation.annotationType()));
        }
        FFIForeignType ffiForeignType = clz.getAnnotation(FFIForeignType.class);
        if (ffiForeignType == null) {
            logger.error("No FFIForeign type annotation found");
            return null;
        }
        return ffiForeignType.value();
    }
}
