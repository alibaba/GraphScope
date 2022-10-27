package com.alibaba.graphscope.arrow.array;

public class ArrowStringArrayBuilder_cxx_0xb067eed0Factory implements ArrowStringArrayBuilder.Factory {
  public static final ArrowStringArrayBuilder.Factory INSTANCE;

  static {
    INSTANCE = new ArrowStringArrayBuilder_cxx_0xb067eed0Factory();
  }

  public ArrowStringArrayBuilder_cxx_0xb067eed0Factory() {
  }

  public ArrowStringArrayBuilder create() {
    return new ArrowStringArrayBuilder_cxx_0xb067eed0(ArrowStringArrayBuilder_cxx_0xb067eed0.nativeCreateFactory0());
  }
}
