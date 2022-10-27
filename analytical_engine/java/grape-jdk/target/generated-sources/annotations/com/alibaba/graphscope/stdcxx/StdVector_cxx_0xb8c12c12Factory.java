package com.alibaba.graphscope.stdcxx;

import java.lang.Byte;

public class StdVector_cxx_0xb8c12c12Factory implements StdVector.Factory<Byte> {
  public static final StdVector.Factory<Byte> INSTANCE;

  static {
    INSTANCE = new StdVector_cxx_0xb8c12c12Factory();
  }

  public StdVector_cxx_0xb8c12c12Factory() {
  }

  public StdVector<Byte> create() {
    return new StdVector_cxx_0xb8c12c12(StdVector_cxx_0xb8c12c12.nativeCreateFactory0());
  }
}
