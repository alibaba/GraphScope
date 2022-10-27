package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "grape::Vertex<uint64_t>",
    factory = Vertex_cxx_0xaccf3424Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.Vertex")
public class Vertex_cxx_0xaccf3424 extends FFIPointerImpl implements Vertex<Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(Vertex_cxx_0xaccf3424.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public Vertex_cxx_0xaccf3424(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Vertex_cxx_0xaccf3424 that = (Vertex_cxx_0xaccf3424) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public Long GetValue() {
    return new java.lang.Long(native_GetValue(address));
  }

  public static native long native_GetValue(long ptr);

  public void SetValue(Long id) {
    native_SetValue(address, id);
  }

  public static native void native_SetValue(long ptr, long id0);

  @CXXOperator("*&")
  @CXXValue
  public Vertex<Long> copy() {
    long ret$ = nativeCopy(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.ds.Vertex_cxx_0xaccf3424.SIZE)); return (new com.alibaba.graphscope.ds.Vertex_cxx_0xaccf3424(ret$));
  }

  @CXXOperator("*&")
  @CXXValue
  public static native long nativeCopy(long ptr, long rv_base);

  @CXXOperator("delete")
  public void delete() {
    nativeDelete(address);
  }

  @CXXOperator("delete")
  public static native void nativeDelete(long ptr);

  @CXXOperator("==")
  public boolean eq(@CXXReference Vertex<Long> vertex) {
    return nativeEq(address, ((com.alibaba.fastffi.FFIPointerImpl) vertex).address);
  }

  @CXXOperator("==")
  public static native boolean nativeEq(long ptr, long vertex0);

  @CXXOperator("++")
  @CXXReference
  public Vertex<Long> inc() {
    long ret$ = nativeInc(address); return (new com.alibaba.graphscope.ds.Vertex_cxx_0xaccf3424(ret$));
  }

  @CXXOperator("++")
  @CXXReference
  public static native long nativeInc(long ptr);

  public static native long nativeCreateFactory0();
}
