package com.alibaba.graphscope.arrow.array;

import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastffi.impl.CXXStdString;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::ArrowStringArray")
@FFISynthetic("com.alibaba.graphscope.arrow.array.ArrowStringArray")
public class ArrowStringArray_cxx_0x49d2fe4b extends FFIPointerImpl implements ArrowStringArray {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ArrowStringArray_cxx_0x49d2fe4b.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ArrowStringArray_cxx_0x49d2fe4b(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrowStringArray_cxx_0x49d2fe4b that = (ArrowStringArray_cxx_0x49d2fe4b) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("GetString")
  @CXXValue
  public CXXStdString getString(long index) {
    long ret$ = nativeGetString(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274.SIZE), index); return (new com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274(ret$));
  }

  @FFINameAlias("GetString")
  @CXXValue
  public static native long nativeGetString(long ptr, long rv_base, long index0);
}
