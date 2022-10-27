package com.alibaba.graphscope.parallel.message;

public class DoubleMsg_cxx_0xc75dfe24Factory implements DoubleMsg.Factory {
  public static final DoubleMsg.Factory INSTANCE;

  static {
    INSTANCE = new DoubleMsg_cxx_0xc75dfe24Factory();
  }

  public DoubleMsg_cxx_0xc75dfe24Factory() {
  }

  public DoubleMsg create() {
    return new DoubleMsg_cxx_0xc75dfe24(DoubleMsg_cxx_0xc75dfe24.nativeCreateFactory0());
  }

  public DoubleMsg create(double inData) {
    return new DoubleMsg_cxx_0xc75dfe24(DoubleMsg_cxx_0xc75dfe24.nativeCreateFactory1(inData));
  }
}
