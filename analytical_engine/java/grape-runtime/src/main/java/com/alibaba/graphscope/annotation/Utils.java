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
