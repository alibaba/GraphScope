package com.alibaba.graphscope.parallel.message;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::LongMsg",
    factory = LongMsg_cxx_0x3d88ac99Factory.class
)
@FFISynthetic("com.alibaba.graphscope.parallel.message.LongMsg")
public class LongMsg_cxx_0x3d88ac99 extends FFIPointerImpl implements LongMsg {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(LongMsg_cxx_0x3d88ac99.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public LongMsg_cxx_0x3d88ac99(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LongMsg_cxx_0x3d88ac99 that = (LongMsg_cxx_0x3d88ac99) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public long getData() {
    return nativeGetData(address);
  }

  public static native long nativeGetData(long ptr);

  public void setData(long value) {
    nativeSetData(address, value);
  }

  public static native void nativeSetData(long ptr, long value0);

  public static native long nativeCreateFactory0();

  public static native long nativeCreateFactory1(long inData0);
}
