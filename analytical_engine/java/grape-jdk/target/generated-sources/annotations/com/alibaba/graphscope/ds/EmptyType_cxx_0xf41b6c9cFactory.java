package com.alibaba.graphscope.ds;

public class EmptyType_cxx_0xf41b6c9cFactory implements EmptyType.Factory {
  public static final EmptyType.Factory INSTANCE;

  static {
    INSTANCE = new EmptyType_cxx_0xf41b6c9cFactory();
  }

  public EmptyType_cxx_0xf41b6c9cFactory() {
  }

  public EmptyType create() {
    return new EmptyType_cxx_0xf41b6c9c(EmptyType_cxx_0xf41b6c9c.nativeCreateFactory0());
  }
}
