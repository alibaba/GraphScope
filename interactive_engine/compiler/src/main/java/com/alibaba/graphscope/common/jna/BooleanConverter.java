package com.alibaba.graphscope.common.jna;

import com.sun.jna.FromNativeContext;
import com.sun.jna.ToNativeContext;
import com.sun.jna.TypeConverter;

// to convert java boolean to i32 and define i32 as the native bool type
public class BooleanConverter implements TypeConverter {
    @Override
    public Object toNative(Object value, ToNativeContext context) {
        return Integer.valueOf(Boolean.TRUE.equals(value) ? 1 : 0);
    }

    @Override
    public Object fromNative(Object value, FromNativeContext context) {
        return ((Integer) value).intValue() != 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override
    public Class<?> nativeType() {
        // BOOL is 32-bit int
        return Integer.class;
    }
}
