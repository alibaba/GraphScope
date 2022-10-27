package com.alibaba.fastffi.impl;

import com.alibaba.fastffi.FFIVector;
import java.lang.Long;

public class CXXStdVector_cxx_0xb71ddcfaFactory implements CXXStdVector.Factory<FFIVector<Long>> {
  public static final CXXStdVector.Factory<FFIVector<Long>> INSTANCE;

  static {
    INSTANCE = new CXXStdVector_cxx_0xb71ddcfaFactory();
  }

  public CXXStdVector_cxx_0xb71ddcfaFactory() {
  }

  public CXXStdVector<FFIVector<Long>> create() {
    return new CXXStdVector_cxx_0xb71ddcfa(CXXStdVector_cxx_0xb71ddcfa.nativeCreateFactory0());
  }

  public CXXStdVector<FFIVector<Long>> create(int arg0) {
    return new CXXStdVector_cxx_0xb71ddcfa(CXXStdVector_cxx_0xb71ddcfa.nativeCreateFactory1(arg0));
  }
}
