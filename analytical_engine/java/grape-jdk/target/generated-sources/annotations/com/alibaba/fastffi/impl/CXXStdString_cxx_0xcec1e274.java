package com.alibaba.fastffi.impl;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import java.lang.Object;
import java.lang.String;

@FFIForeignType(
    value = "std::string",
    factory = CXXStdString_cxx_0xcec1e274Factory.class
)
@FFISynthetic("com.alibaba.fastffi.impl.CXXStdString")
public class CXXStdString_cxx_0xcec1e274 extends FFIPointerImpl implements CXXStdString {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public CXXStdString_cxx_0xcec1e274(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CXXStdString_cxx_0xcec1e274 that = (CXXStdString_cxx_0xcec1e274) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return toJavaString();
  }

  @CXXOperator("[]")
  public byte byteAt(long arg0) {
    return nativeByteAt(address, arg0);
  }

  @CXXOperator("[]")
  public static native byte nativeByteAt(long ptr, long arg00);

  public void clear() {
    nativeClear(address);
  }

  public static native void nativeClear(long ptr);

  public long data() {
    return nativeData(address);
  }

  public static native long nativeData(long ptr);

  public void reserve(long arg0) {
    nativeReserve(address, arg0);
  }

  public static native void nativeReserve(long ptr, long arg00);

  public void resize(long arg0) {
    nativeResize(address, arg0);
  }

  public static native void nativeResize(long ptr, long arg00);

  public long size() {
    return nativeSize(address);
  }

  public static native long nativeSize(long ptr);

  public static native long nativeCreateFactory0();
}
