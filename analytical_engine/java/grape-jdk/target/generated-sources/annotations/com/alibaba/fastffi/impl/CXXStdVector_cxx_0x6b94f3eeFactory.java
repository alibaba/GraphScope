package com.alibaba.fastffi.impl;

import java.lang.Long;

public class CXXStdVector_cxx_0x6b94f3eeFactory implements CXXStdVector.Factory<Long> {
  public static final CXXStdVector.Factory<Long> INSTANCE;

  static {
    INSTANCE = new CXXStdVector_cxx_0x6b94f3eeFactory();
  }

  public CXXStdVector_cxx_0x6b94f3eeFactory() {
  }

  public CXXStdVector<Long> create() {
    return new CXXStdVector_cxx_0x6b94f3ee(CXXStdVector_cxx_0x6b94f3ee.nativeCreateFactory0());
  }

  public CXXStdVector<Long> create(int arg0) {
    return new CXXStdVector_cxx_0x6b94f3ee(CXXStdVector_cxx_0x6b94f3ee.nativeCreateFactory1(arg0));
  }
}
