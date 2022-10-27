package com.alibaba.fastffi.impl;

import com.alibaba.fastffi.FFIVector;
import java.lang.Integer;

public class CXXStdVector_cxx_0xbb6af271Factory implements CXXStdVector.Factory<FFIVector<Integer>> {
  public static final CXXStdVector.Factory<FFIVector<Integer>> INSTANCE;

  static {
    INSTANCE = new CXXStdVector_cxx_0xbb6af271Factory();
  }

  public CXXStdVector_cxx_0xbb6af271Factory() {
  }

  public CXXStdVector<FFIVector<Integer>> create() {
    return new CXXStdVector_cxx_0xbb6af271(CXXStdVector_cxx_0xbb6af271.nativeCreateFactory0());
  }

  public CXXStdVector<FFIVector<Integer>> create(int arg0) {
    return new CXXStdVector_cxx_0xbb6af271(CXXStdVector_cxx_0xbb6af271.nativeCreateFactory1(arg0));
  }
}
