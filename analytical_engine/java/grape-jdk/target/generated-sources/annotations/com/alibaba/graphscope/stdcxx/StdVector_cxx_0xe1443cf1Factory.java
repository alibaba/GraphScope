package com.alibaba.graphscope.stdcxx;

import java.lang.Integer;

public class StdVector_cxx_0xe1443cf1Factory implements StdVector.Factory<Integer> {
  public static final StdVector.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new StdVector_cxx_0xe1443cf1Factory();
  }

  public StdVector_cxx_0xe1443cf1Factory() {
  }

  public StdVector<Integer> create() {
    return new StdVector_cxx_0xe1443cf1(StdVector_cxx_0xe1443cf1.nativeCreateFactory0());
  }
}
