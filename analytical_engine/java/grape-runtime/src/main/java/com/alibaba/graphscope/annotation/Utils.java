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

import static com.alibaba.graphscope.utils.CppClassName.DOUBLE_COLUMN;
import static com.alibaba.graphscope.utils.CppClassName.INT_COLUMN;
import static com.alibaba.graphscope.utils.CppClassName.LONG_COLUMN;

import com.alibaba.fastffi.CXXTemplate;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.graphscope.column.DoubleColumn;
import com.alibaba.graphscope.column.IntColumn;
import com.alibaba.graphscope.column.LongColumn;
import com.alibaba.graphscope.ds.StringView;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import com.squareup.javapoet.AnnotationSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Providing useful utility functions for build AnnotationSpec builder.
 */
public class Utils {

    public static void addIntCXXTemplate(AnnotationSpec.Builder builder) {
        builder.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint32_t")
                        .addMember("java", "$S", "Integer")
                        .build());
    }

    public static void addLongCXXTemplate(AnnotationSpec.Builder buider) {
        buider.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "uint64_t")
                        .addMember("java", "$S", "Long")
                        .build());
    }

    public static void addSignedIntCXXTemplate(AnnotationSpec.Builder builder) {
        builder.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "int32_t")
                        .addMember("java", "$S", "Integer")
                        .build());
    }

    public static void addSignedLongCXXTemplate(AnnotationSpec.Builder builder) {
        builder.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "int64_t")
                        .addMember("java", "$S", "Long")
                        .build());
    }

    public static void addDoubleCXXTemplate(AnnotationSpec.Builder builder) {
        builder.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", "double")
                        .addMember("java", "$S", "Double")
                        .build());
    }

    public static void addCXXTemplate(
            AnnotationSpec.Builder builder,
            String foreignFirst,
            String foreignSecond,
            String javaFirst,
            String javaSecond) {
        builder.addMember(
                "templates",
                "$L",
                AnnotationSpec.builder(CXXTemplate.class)
                        .addMember("cxx", "$S", foreignFirst)
                        .addMember("cxx", "$S", foreignSecond)
                        .addMember("java", "$S", javaFirst)
                        .addMember("java", "$S", javaSecond)
                        .build());
    }

    public static void addColumn(
            AnnotationSpec.Builder batchBuilder, String foreignFragName, String javaFragName) {
        for (String columnName :
                new String[] {
                    DoubleColumn.class.getName(),
                    LongColumn.class.getName(),
                    IntColumn.class.getName()
                }) {
            AnnotationSpec.Builder ffiGenVertex = AnnotationSpec.builder(FFIGen.class);
            ffiGenVertex.addMember("type", "$S", columnName);
            ffiGenVertex.addMember(
                    "templates",
                    "$L",
                    AnnotationSpec.builder(CXXTemplate.class)
                            .addMember("cxx", "$S", foreignFragName)
                            .addMember("java", "$S", javaFragName)
                            .build());
            batchBuilder.addMember("value", "$L", ffiGenVertex.build());
        }
    }

    public static void addSharedPtr(
            AnnotationSpec.Builder batchBuilder, String foreignFragName, String javaFragName) {
        {
            AnnotationSpec.Builder ffiGenSharedPtr = AnnotationSpec.builder(FFIGen.class);
            ffiGenSharedPtr.addMember("type", "$S", StdSharedPtr.class.getName());
            for (String[] columnTypePair :
                    new String[][] {
                        {DOUBLE_COLUMN, DoubleColumn.class.getName()},
                        {INT_COLUMN, IntColumn.class.getName()},
                        {LONG_COLUMN, LongColumn.class.getName()}
                    }) {
                ffiGenSharedPtr.addMember(
                        "templates",
                        "$L",
                        AnnotationSpec.builder(CXXTemplate.class)
                                .addMember(
                                        "cxx",
                                        "$S",
                                        makeParameterizedType(columnTypePair[0], foreignFragName))
                                .addMember(
                                        "java",
                                        "$S",
                                        makeParameterizedType(columnTypePair[1], javaFragName))
                                .build());
            }
            batchBuilder.addMember("value", "$L", ffiGenSharedPtr.build());
        }
    }

    public static String makeParameterizedType(String base, String... types) {
        if (types.length == 0) {
            return base;
        }
        return base + "<" + String.join(",", types) + ">";
    }

    public static void vertexDataContextAddTemplate(
            AnnotationSpec.Builder vertexDataContextBuilder,
            String foreignFragName,
            String javaFragName) {
        for (String[] dataType :
                new String[][] {
                    {"int64_t", Long.class.getName()},
                    {"int32_t", Integer.class.getName()},
                    {"double", Double.class.getName()}
                }) {
            vertexDataContextBuilder.addMember(
                    "templates",
                    "$L",
                    AnnotationSpec.builder(CXXTemplate.class)
                            .addMember("cxx", "$S", foreignFragName)
                            .addMember("cxx", "$S", dataType[0])
                            .addMember("java", "$S", javaFragName)
                            .addMember("java", "$S", dataType[1])
                            .build());
        }
    }

    /**
     * This property should be already set in environment.
     * @return
     */
    public static List<String> getMessageTypes() {
        String messageTypeString = System.getProperty("grape.messageTypes");
        if (messageTypeString == null || messageTypeString.isEmpty()) {
            throw new IllegalStateException("no property grape.messageTypes found ");
        }
        List<String> messageTypes = new ArrayList<>();
        if (messageTypeString != null && !messageTypeString.isEmpty()) {
            Arrays.asList(parseMessageTypes(messageTypeString)).forEach(p -> messageTypes.add(p));
        }
        return messageTypes;
    }

    /**
     * Use : to separate types
     *
     * @param messageTypes
     * @return
     */
    public static String[] parseMessageTypes(String messageTypes) {
        String[] results =
                Arrays.stream(messageTypes.split(",")).map(m -> m.trim()).toArray(String[]::new);
        return results;
    }

    public static String cpp2Java(String cppType) {
        if (cppType.equals("int64_t") || cppType.equals("uint64_t")) {
            return Long.class.getName();
        } else if (cppType.equals("int32_t") || cppType.equals("uint32_t")) {
            return Integer.class.getName();
        } else if (cppType.equals("double")) {
            return Double.class.getName();
        } else if (cppType.equals("std::string")) {
            return StringView.class.getName();
        }
        throw new IllegalStateException("Not recognized cpp " + cppType);
    }

    public static String java2Cpp(String javaType, boolean signed) {
        if (javaType.equals("java.lang.Long") || javaType.equals("Long")) {
            if (signed) {
                return "int64_t";
            } else return "uint64_t";
        } else if (javaType.equals("java.lang.Integer") || javaType.equals("Integer")) {
            if (signed) {
                return "int32_t";
            } else return "uint32_t";
        } else if (javaType.equals("java.lang.Double") || javaType.equals("Double")) {
            return "double";
        } else if (javaType.equals("com.alibaba.graphscope.ds.StringView")) {
            return "std::string";
        }
        throw new IllegalStateException("Unrecognized type " + javaType + " sign: " + signed);
    }
}
