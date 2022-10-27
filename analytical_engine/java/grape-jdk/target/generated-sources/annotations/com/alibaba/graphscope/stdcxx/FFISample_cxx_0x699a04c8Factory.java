package com.alibaba.graphscope.stdcxx;

public class FFISample_cxx_0x699a04c8Factory implements FFISample.Factory {
  public static final FFISample.Factory INSTANCE;

  static {
    INSTANCE = new FFISample_cxx_0x699a04c8Factory();
  }

  public FFISample_cxx_0x699a04c8Factory() {
  }

  public FFISample create() {
    return new FFISample_cxx_0x699a04c8(FFISample_cxx_0x699a04c8.nativeCreateFactory0());
  }

  public FFISample createStack() {
    return new FFISample_cxx_0x699a04c8(FFISample_cxx_0x699a04c8.nativeCreateStackFactory1(com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.FFISample_cxx_0x699a04c8.SIZE)));
  }
}
