package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "double",
    factory = DoublePointer_cxx_0xb0f77bd1Factory.class
)
@FFISynthetic("com.alibaba.graphscope.stdcxx.DoublePointer")
public class DoublePointer_cxx_0xb0f77bd1 extends FFIPointerImpl implements DoublePointer {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(DoublePointer_cxx_0xb0f77bd1.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public DoublePointer_cxx_0xb0f77bd1(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DoublePointer_cxx_0xb0f77bd1 that = (DoublePointer_cxx_0xb0f77bd1) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXOperator("=")
  public void SetValue(double value) {
    native_SetValue(address, value);
  }

  @CXXOperator("=")
  public static native void native_SetValue(long ptr, double value0);

  @CXXOperator("delete")
  public void delete() {
    nativeDelete(address);
  }

  @CXXOperator("delete")
  public static native void nativeDelete(long ptr);

  @CXXOperator("*&")
  public double toDouble() {
    return nativeToDouble(address);
  }

  @CXXOperator("*&")
  public static native double nativeToDouble(long ptr);

  public static native long nativeCreateFactory0();
}
