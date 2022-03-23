package com.alibaba.graphscope.stdcxx;

public class FFIByteVecVectorFactory implements StdVector.Factory<StdVector<Byte>> {
    public static final StdVector.Factory<StdVector<Byte>> INSTANCE;

    static {
        INSTANCE = new FFIByteVecVectorFactory();
    }

    public FFIByteVecVectorFactory() {}

    public StdVector<StdVector<Byte>> create() {
        return new FFIByteVecVector(FFIByteVecVector.nativeCreateFactory0());
    }
}
