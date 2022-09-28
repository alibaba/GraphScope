package com.alibaba.graphscope.utils;

import com.alibaba.fastffi.llvm4jni.runtime.JavaRuntime;

public class LongPointerAccessor {
    private final long address;

    public LongPointerAccessor(long address) {
        this.address = address;
    }

    public long get(int index) {
        return JavaRuntime.getLong(address + ((long) index << 3));
    }

    public long get(long index) {
        return JavaRuntime.getLong(address + (index << 3));
    }
}
