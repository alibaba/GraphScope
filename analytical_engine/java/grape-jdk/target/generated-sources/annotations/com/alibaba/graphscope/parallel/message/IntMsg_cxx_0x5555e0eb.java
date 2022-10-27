package com.alibaba.graphscope.parallel.message;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::PrimitiveMessage<int32_t>",
    factory = IntMsg_cxx_0x5555e0ebFactory.class
)
@FFISynthetic("com.alibaba.graphscope.parallel.message.IntMsg")
public class IntMsg_cxx_0x5555e0eb extends FFIPointerImpl implements IntMsg {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(IntMsg_cxx_0x5555e0eb.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public IntMsg_cxx_0x5555e0eb(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IntMsg_cxx_0x5555e0eb that = (IntMsg_cxx_0x5555e0eb) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public int getData() {
    return nativeGetData(address);
  }

  public static native int nativeGetData(long ptr);

  public void setData(int value) {
    nativeSetData(address, value);
  }

  public static native void nativeSetData(long ptr, int value0);

  public static native long nativeCreateFactory0();

  public static native long nativeCreateFactory1(int inData0);
}
