package com.alibaba.fastffi.impl;

import java.lang.Byte;

public class CXXStdVector_cxx_0x6b0caae2Factory implements CXXStdVector.Factory<Byte> {
  public static final CXXStdVector.Factory<Byte> INSTANCE;

  static {
    INSTANCE = new CXXStdVector_cxx_0x6b0caae2Factory();
  }

  public CXXStdVector_cxx_0x6b0caae2Factory() {
  }

  public CXXStdVector<Byte> create() {
    return new CXXStdVector_cxx_0x6b0caae2(CXXStdVector_cxx_0x6b0caae2.nativeCreateFactory0());
  }

  public CXXStdVector<Byte> create(int arg0) {
    return new CXXStdVector_cxx_0x6b0caae2(CXXStdVector_cxx_0x6b0caae2.nativeCreateFactory1(arg0));
  }
}
