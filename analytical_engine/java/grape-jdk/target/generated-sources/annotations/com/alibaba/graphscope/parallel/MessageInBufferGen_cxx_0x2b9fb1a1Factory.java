package com.alibaba.graphscope.parallel;

public class MessageInBufferGen_cxx_0x2b9fb1a1Factory implements MessageInBuffer.Factory {
  public static final MessageInBuffer.Factory INSTANCE;

  static {
    INSTANCE = new MessageInBufferGen_cxx_0x2b9fb1a1Factory();
  }

  public MessageInBufferGen_cxx_0x2b9fb1a1Factory() {
  }

  public MessageInBuffer create() {
    return new MessageInBufferGen_cxx_0x2b9fb1a1(MessageInBufferGen_cxx_0x2b9fb1a1.nativeCreateFactory0());
  }
}
