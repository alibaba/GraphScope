package com.alibaba.fastffi.impl;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import java.lang.Byte;
import java.lang.Object;
import java.lang.String;

@FFIForeignType(
    value = "std::vector<jbyte>",
    factory = CXXStdVector_cxx_0x6b0caae2Factory.class
)
@FFISynthetic("com.alibaba.fastffi.impl.CXXStdVector")
public class CXXStdVector_cxx_0x6b0caae2 extends FFIPointerImpl implements CXXStdVector<Byte> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public CXXStdVector_cxx_0x6b0caae2(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CXXStdVector_cxx_0x6b0caae2 that = (CXXStdVector_cxx_0x6b0caae2) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public long capacity() {
    return nativeCapacity(address);
  }

  public static native long nativeCapacity(long ptr);

  public void clear() {
    nativeClear(address);
  }

  public static native void nativeClear(long ptr);

  public long data() {
    return nativeData(address);
  }

  public static native long nativeData(long ptr);

  @CXXOperator("delete")
  public void dispose() {
    nativeDispose(address);
  }

  @CXXOperator("delete")
  public static native void nativeDispose(long ptr);

  public boolean empty() {
    return nativeEmpty(address);
  }

  public static native boolean nativeEmpty(long ptr);

  @CXXOperator("[]")
  @CXXReference
  public Byte get(long arg0) {
    return nativeGet(address, arg0);
  }

  @CXXOperator("[]")
  @CXXReference
  public static native byte nativeGet(long ptr, long arg00);

  public void push_back(@CXXReference Byte arg0) {
    nativePush_back(address, arg0);
  }

  public static native void nativePush_back(long ptr, byte arg00);

  public void reserve(long arg0) {
    nativeReserve(address, arg0);
  }

  public static native void nativeReserve(long ptr, long arg00);

  public void resize(long arg0) {
    nativeResize(address, arg0);
  }

  public static native void nativeResize(long ptr, long arg00);

  @CXXOperator("[]")
  public void set(long arg0, @CXXReference Byte arg1) {
    nativeSet(address, arg0, arg1);
  }

  @CXXOperator("[]")
  public static native void nativeSet(long ptr, long arg00, byte arg11);

  public long size() {
    return nativeSize(address);
  }

  public static native long nativeSize(long ptr);

  public static native long nativeCreateFactory0();

  public static native long nativeCreateFactory1(int arg00);
}
