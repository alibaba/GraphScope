package com.alibaba.graphscope.ds;

public class Bitset_cxx_0x2967d1e0Factory implements Bitset.Factory {
  public static final Bitset.Factory INSTANCE;

  static {
    INSTANCE = new Bitset_cxx_0x2967d1e0Factory();
  }

  public Bitset_cxx_0x2967d1e0Factory() {
  }

  public Bitset create() {
    return new Bitset_cxx_0x2967d1e0(Bitset_cxx_0x2967d1e0.nativeCreateFactory0());
  }
}
