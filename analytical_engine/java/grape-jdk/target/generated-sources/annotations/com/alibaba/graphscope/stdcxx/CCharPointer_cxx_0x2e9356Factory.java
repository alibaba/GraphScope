package com.alibaba.graphscope.stdcxx;

public class CCharPointer_cxx_0x2e9356Factory implements CCharPointer.Factory {
  public static final CCharPointer.Factory INSTANCE;

  static {
    INSTANCE = new CCharPointer_cxx_0x2e9356Factory();
  }

  public CCharPointer_cxx_0x2e9356Factory() {
  }

  public CCharPointer create() {
    return new CCharPointer_cxx_0x2e9356(CCharPointer_cxx_0x2e9356.nativeCreateFactory0());
  }
}
