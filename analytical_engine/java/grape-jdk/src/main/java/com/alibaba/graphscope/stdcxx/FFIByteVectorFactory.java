package com.alibaba.graphscope.stdcxx;

public class FFIByteVectorFactory implements StdVector.Factory<Byte> {
    public static final StdVector.Factory<Byte> INSTANCE;

    static {
        INSTANCE = new FFIByteVectorFactory();
    }

    public FFIByteVectorFactory() {}

    public StdVector<Byte> create() {
        return new FFIByteVector(FFIByteVector.nativeCreateFactory0());
    }
}
