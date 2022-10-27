package com.alibaba.graphscope.parallel.message;

public class LongMsg_cxx_0x3d88ac99Factory implements LongMsg.Factory {
  public static final LongMsg.Factory INSTANCE;

  static {
    INSTANCE = new LongMsg_cxx_0x3d88ac99Factory();
  }

  public LongMsg_cxx_0x3d88ac99Factory() {
  }

  public LongMsg create() {
    return new LongMsg_cxx_0x3d88ac99(LongMsg_cxx_0x3d88ac99.nativeCreateFactory0());
  }

  public LongMsg create(long inData) {
    return new LongMsg_cxx_0x3d88ac99(LongMsg_cxx_0x3d88ac99.nativeCreateFactory1(inData));
  }
}
