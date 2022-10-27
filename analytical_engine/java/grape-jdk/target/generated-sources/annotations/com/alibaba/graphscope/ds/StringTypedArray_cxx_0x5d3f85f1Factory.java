package com.alibaba.graphscope.ds;

public class StringTypedArray_cxx_0x5d3f85f1Factory implements StringTypedArray.Factory {
  public static final StringTypedArray.Factory INSTANCE;

  static {
    INSTANCE = new StringTypedArray_cxx_0x5d3f85f1Factory();
  }

  public StringTypedArray_cxx_0x5d3f85f1Factory() {
  }

  public StringTypedArray create() {
    return new StringTypedArray_cxx_0x5d3f85f1(StringTypedArray_cxx_0x5d3f85f1.nativeCreateFactory0());
  }
}
