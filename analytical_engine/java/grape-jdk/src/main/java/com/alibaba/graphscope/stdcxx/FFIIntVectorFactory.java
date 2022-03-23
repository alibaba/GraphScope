package com.alibaba.graphscope.stdcxx;

public class FFIIntVectorFactory implements StdVector.Factory<Integer> {
    public static final StdVector.Factory<Integer> INSTANCE;

    static {
        INSTANCE = new FFIIntVectorFactory();
    }

    public FFIIntVectorFactory() {}

    public StdVector<Integer> create() {
        return new FFIIntVector(FFIIntVector.nativeCreateFactory0());
    }
}
