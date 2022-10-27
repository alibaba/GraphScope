package com.alibaba.fastffi.impl;

public class CXXStdString_cxx_0xcec1e274Factory implements CXXStdString.Factory {
  public static final CXXStdString.Factory INSTANCE;

  static {
    INSTANCE = new CXXStdString_cxx_0xcec1e274Factory();
  }

  public CXXStdString_cxx_0xcec1e274Factory() {
  }

  public CXXStdString create() {
    return new CXXStdString_cxx_0xcec1e274(CXXStdString_cxx_0xcec1e274.nativeCreateFactory0());
  }
}
