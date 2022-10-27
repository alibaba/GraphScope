package com.alibaba.graphscope.stdcxx;

public class StdString_cxx_0xcec1e274Factory implements StdString.Factory {
  public static final StdString.Factory INSTANCE;

  static {
    INSTANCE = new StdString_cxx_0xcec1e274Factory();
  }

  public StdString_cxx_0xcec1e274Factory() {
  }

  public StdString create() {
    return new StdString_cxx_0xcec1e274(StdString_cxx_0xcec1e274.nativeCreateFactory0());
  }

  public StdString create(StdString string) {
    return new StdString_cxx_0xcec1e274(StdString_cxx_0xcec1e274.nativeCreateFactory1(((com.alibaba.fastffi.FFIPointerImpl) string).address));
  }
}
