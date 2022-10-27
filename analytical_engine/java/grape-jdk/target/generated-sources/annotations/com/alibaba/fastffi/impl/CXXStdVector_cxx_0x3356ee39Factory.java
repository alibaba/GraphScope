package com.alibaba.fastffi.impl;

import java.lang.Double;

public class CXXStdVector_cxx_0x3356ee39Factory implements CXXStdVector.Factory<Double> {
  public static final CXXStdVector.Factory<Double> INSTANCE;

  static {
    INSTANCE = new CXXStdVector_cxx_0x3356ee39Factory();
  }

  public CXXStdVector_cxx_0x3356ee39Factory() {
  }

  public CXXStdVector<Double> create() {
    return new CXXStdVector_cxx_0x3356ee39(CXXStdVector_cxx_0x3356ee39.nativeCreateFactory0());
  }

  public CXXStdVector<Double> create(int arg0) {
    return new CXXStdVector_cxx_0x3356ee39(CXXStdVector_cxx_0x3356ee39.nativeCreateFactory1(arg0));
  }
}
