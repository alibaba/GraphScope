package com.alibaba.graphscope.stdcxx;

public class FFIIntVecVectorFactory implements StdVector.Factory<StdVector<Integer>> {
    public static final StdVector.Factory<StdVector<Integer>> INSTANCE;

    static {
        INSTANCE = new FFIIntVecVectorFactory();
    }

    public FFIIntVecVectorFactory() {}

    public StdVector<StdVector<Integer>> create() {
        return new FFIIntVecVector(FFIIntVecVector.nativeCreateFactory0());
    }
}
