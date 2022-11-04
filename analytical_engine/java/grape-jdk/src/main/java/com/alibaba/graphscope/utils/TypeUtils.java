package com.alibaba.graphscope.utils;

public class TypeUtils {

    public static boolean isPrimitive(Class<?> javaClass) {
        if (javaClass.isPrimitive()) {
            return true;
        }
        return javaClass.equals(Long.class)
                || javaClass.equals(Double.class)
                || javaClass.equals(Integer.class)
                || javaClass.equals(Float.class);
    }

    public static String primitiveClass2CppStr(Class<?> javaClass, boolean sign) {
        if (javaClass.equals(Long.class) || javaClass.equals(long.class)) {
            if (sign) {
                return "int64_t";
            } else {
                return "uint64_t";
            }
        } else if (javaClass.equals(Integer.class) || javaClass.equals(int.class)) {
            if (sign) {
                return "int32_t";
            } else {
                return "uint32_t";
            }
        } else if (javaClass.equals(Double.class) || javaClass.equals(double.class)) {
            return "double";
        } else if (javaClass.equals(Float.class) || javaClass.equals(float.class)) {
            return "float";
        } else {
            throw new IllegalStateException("Not recognized class " + javaClass.getName());
        }
    }
}
