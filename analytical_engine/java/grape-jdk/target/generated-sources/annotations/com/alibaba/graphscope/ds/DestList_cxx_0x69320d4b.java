package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("grape::DestList")
@FFISynthetic("com.alibaba.graphscope.ds.DestList")
public class DestList_cxx_0x69320d4b extends FFIPointerImpl implements DestList {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(DestList_cxx_0x69320d4b.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public DestList_cxx_0x69320d4b(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DestList_cxx_0x69320d4b that = (DestList_cxx_0x69320d4b) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFIGetter
  public FidPointer begin() {
    long ret$ = nativeBegin(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.ds.FidPointer_cxx_0xff0c66f5(ret$));
  }

  @FFIGetter
  public static native long nativeBegin(long ptr);

  @CXXOperator("delete")
  public void delete() {
    nativeDelete(address);
  }

  @CXXOperator("delete")
  public static native void nativeDelete(long ptr);

  @FFIGetter
  public FidPointer end() {
    long ret$ = nativeEnd(address); return (ret$ == 0L ? null : new com.alibaba.graphscope.ds.FidPointer_cxx_0xff0c66f5(ret$));
  }

  @FFIGetter
  public static native long nativeEnd(long ptr);
}
