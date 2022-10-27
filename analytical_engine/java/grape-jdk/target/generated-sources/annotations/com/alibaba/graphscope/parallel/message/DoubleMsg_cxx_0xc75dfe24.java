package com.alibaba.graphscope.parallel.message;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::DoubleMsg",
    factory = DoubleMsg_cxx_0xc75dfe24Factory.class
)
@FFISynthetic("com.alibaba.graphscope.parallel.message.DoubleMsg")
public class DoubleMsg_cxx_0xc75dfe24 extends FFIPointerImpl implements DoubleMsg {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(DoubleMsg_cxx_0xc75dfe24.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public DoubleMsg_cxx_0xc75dfe24(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DoubleMsg_cxx_0xc75dfe24 that = (DoubleMsg_cxx_0xc75dfe24) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public double getData() {
    return nativeGetData(address);
  }

  public static native double nativeGetData(long ptr);

  public void setData(double value) {
    nativeSetData(address, value);
  }

  public static native void nativeSetData(long ptr, double value0);

  public static native long nativeCreateFactory0();

  public static native long nativeCreateFactory1(double inData0);
}
