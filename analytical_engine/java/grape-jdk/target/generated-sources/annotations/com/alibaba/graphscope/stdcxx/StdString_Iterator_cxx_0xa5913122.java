package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("std::string::iterator")
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdString.Iterator")
public class StdString_Iterator_cxx_0xa5913122 extends FFIPointerImpl implements StdString.Iterator {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StdString_Iterator_cxx_0xa5913122.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StdString_Iterator_cxx_0xa5913122(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StdString_Iterator_cxx_0xa5913122 that = (StdString_Iterator_cxx_0xa5913122) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXOperator("*&")
  @CXXValue
  public StdString.Iterator copy() {
    long ret$ = nativeCopy(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.StdString_Iterator_cxx_0xa5913122.SIZE)); return (new com.alibaba.graphscope.stdcxx.StdString_Iterator_cxx_0xa5913122(ret$));
  }

  @CXXOperator("*&")
  @CXXValue
  public static native long nativeCopy(long ptr, long rv_base);

  @CXXOperator("==")
  public boolean eq(@CXXReference StdString.Iterator rhs) {
    return nativeEq(address, ((com.alibaba.fastffi.FFIPointerImpl) rhs).address);
  }

  @CXXOperator("==")
  public static native boolean nativeEq(long ptr, long rhs0);

  @CXXOperator("++")
  @CXXReference
  public StdString.Iterator inc() {
    long ret$ = nativeInc(address); return (new com.alibaba.graphscope.stdcxx.StdString_Iterator_cxx_0xa5913122(ret$));
  }

  @CXXOperator("++")
  @CXXReference
  public static native long nativeInc(long ptr);

  @CXXOperator("*")
  public byte indirection() {
    return nativeIndirection(address);
  }

  @CXXOperator("*")
  public static native byte nativeIndirection(long ptr);
}
