package com.alibaba.graphscope.annotation;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(value = RUNTIME)
@Target(ElementType.TYPE)
public @interface GrapeSkip {
    String[] vertexDataTypes();

    String[] edgeDataTypes();

    String[] msgDataTypes();
}
