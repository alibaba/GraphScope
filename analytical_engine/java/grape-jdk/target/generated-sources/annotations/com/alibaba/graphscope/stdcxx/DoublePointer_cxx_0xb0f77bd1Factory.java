package com.alibaba.graphscope.stdcxx;

public class DoublePointer_cxx_0xb0f77bd1Factory implements DoublePointer.Factory {
  public static final DoublePointer.Factory INSTANCE;

  static {
    INSTANCE = new DoublePointer_cxx_0xb0f77bd1Factory();
  }

  public DoublePointer_cxx_0xb0f77bd1Factory() {
  }

  public DoublePointer create() {
    return new DoublePointer_cxx_0xb0f77bd1(DoublePointer_cxx_0xb0f77bd1.nativeCreateFactory0());
  }
}
