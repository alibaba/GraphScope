package com.alibaba.graphscope.utils;

import com.alibaba.fastffi.FFIByteString;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A interface which will be subclassed by auto-generated grape skip classes, used to support
 * overloading for ffi method.
 */
public interface Unused {

    static Unused getUnused(Class<?> a, Class<?> b, Class<?> c) {
        try {
            Class<?> implClass = Class.forName("com.alibaba.graphscope.runtime.UnusedImpl");
            Method method = implClass.getMethod("getUnused", Class.class, Class.class, Class.class);
            if (method == null) {
                throw new IllegalStateException("fail to get method");
            }
            return (Unused) method.invoke(null, a, b, c);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException
                | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Get unused variable when no msg is specified
    static Unused getUnused(Class<?> a, Class<?> b) {
        try {
            Class<?> implClass = Class.forName("com.alibaba.graphscope.runtime.UnusedImpl");
            Method method = implClass.getMethod("getUnused", Class.class, Class.class);
            if (method == null) {
                throw new IllegalStateException("fail to get method");
            }
            return (Unused) method.invoke(null, a, b);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException
                | IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    static int class2Int(Class clz) {
        if (clz.equals(Long.class) || clz.getName().contains("LongMsg")) {
            return 0;
        } else if (clz.equals(Double.class) || clz.getName().contains("DoubleMsg")) {
            return 1;
        } else if (clz.equals(Integer.class) || clz.getName().contains("IntMsg")) {
            return 2;
        } else if (clz.equals(String.class)
                || FFIByteString.class.isAssignableFrom(clz)
                || clz.getName().contains("StringView")) {
            return 3;
        } else if (clz.getSimpleName().startsWith("EmptyType")) {
            return 4;
        } else {
            throw new IllegalStateException("Not possible clz" + clz.getName());
        }
    }
}
