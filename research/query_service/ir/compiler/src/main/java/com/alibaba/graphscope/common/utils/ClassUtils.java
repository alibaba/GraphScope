package com.alibaba.graphscope.common.utils;

public class ClassUtils {
    public static <T> boolean equalClass(T t1, Class<? extends T> target) {
        return t1.getClass().equals(target);
    }
}
