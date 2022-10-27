package com.alibaba.fastffi.impl;

import com.alibaba.fastffi.FFIVector;
import java.lang.Double;

public class CXXStdVector_cxx_0x33d2198fFactory implements CXXStdVector.Factory<FFIVector<Double>> {
  public static final CXXStdVector.Factory<FFIVector<Double>> INSTANCE;

  static {
    INSTANCE = new CXXStdVector_cxx_0x33d2198fFactory();
  }

  public CXXStdVector_cxx_0x33d2198fFactory() {
  }

  public CXXStdVector<FFIVector<Double>> create() {
    return new CXXStdVector_cxx_0x33d2198f(CXXStdVector_cxx_0x33d2198f.nativeCreateFactory0());
  }

  public CXXStdVector<FFIVector<Double>> create(int arg0) {
    return new CXXStdVector_cxx_0x33d2198f(CXXStdVector_cxx_0x33d2198f.nativeCreateFactory1(arg0));
  }
}
