package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "grape::MessageInBuffer",
    factory = MessageInBufferGen_cxx_0x2b9fb1a1Factory.class
)
@FFISynthetic("com.alibaba.graphscope.parallel.MessageInBufferGen")
public class MessageInBufferGen_cxx_0x2b9fb1a1 extends FFIPointerImpl implements MessageInBufferGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(MessageInBufferGen_cxx_0x2b9fb1a1.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public MessageInBufferGen_cxx_0x2b9fb1a1(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MessageInBufferGen_cxx_0x2b9fb1a1 that = (MessageInBufferGen_cxx_0x2b9fb1a1) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public static native long nativeCreateFactory0();
}
