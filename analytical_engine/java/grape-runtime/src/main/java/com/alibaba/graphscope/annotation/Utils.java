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

import com.alibaba.fastffi.CXXTemplate;
import com.squareup.javapoet.AnnotationSpec;

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
}
