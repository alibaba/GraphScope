package com.alibaba.graphscope.parallel.message;

public class IntMsg_cxx_0x5555e0ebFactory implements IntMsg.Factory {
  public static final IntMsg.Factory INSTANCE;

  static {
    INSTANCE = new IntMsg_cxx_0x5555e0ebFactory();
  }

  public IntMsg_cxx_0x5555e0ebFactory() {
  }

  public IntMsg create() {
    return new IntMsg_cxx_0x5555e0eb(IntMsg_cxx_0x5555e0eb.nativeCreateFactory0());
  }

  public IntMsg create(int inData) {
    return new IntMsg_cxx_0x5555e0eb(IntMsg_cxx_0x5555e0eb.nativeCreateFactory1(inData));
  }
}
