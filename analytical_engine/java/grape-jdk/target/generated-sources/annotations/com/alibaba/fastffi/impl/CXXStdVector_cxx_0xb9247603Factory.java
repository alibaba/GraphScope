package com.alibaba.fastffi.impl;

import java.lang.Integer;

public class CXXStdVector_cxx_0xb9247603Factory implements CXXStdVector.Factory<Integer> {
  public static final CXXStdVector.Factory<Integer> INSTANCE;

  static {
    INSTANCE = new CXXStdVector_cxx_0xb9247603Factory();
  }

  public CXXStdVector_cxx_0xb9247603Factory() {
  }

  public CXXStdVector<Integer> create() {
    return new CXXStdVector_cxx_0xb9247603(CXXStdVector_cxx_0xb9247603.nativeCreateFactory0());
  }

  public CXXStdVector<Integer> create(int arg0) {
    return new CXXStdVector_cxx_0xb9247603(CXXStdVector_cxx_0xb9247603.nativeCreateFactory1(arg0));
  }
}
