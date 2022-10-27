package com.alibaba.graphscope.column;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("gs::IColumn")
@FFISynthetic("com.alibaba.graphscope.column.IColumn")
public class IColumn_cxx_0x53d1b253 extends FFIPointerImpl implements IColumn {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(IColumn_cxx_0x53d1b253.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public IColumn_cxx_0x53d1b253(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IColumn_cxx_0x53d1b253 that = (IColumn_cxx_0x53d1b253) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXValue
  public FFIByteString name() {
    long ret$ = nativeName(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274.SIZE)); return (new com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274(ret$));
  }

  @CXXValue
  public static native long nativeName(long ptr, long rv_base);

  @FFINameAlias("set_name")
  public void setName(@CXXReference FFIByteString name) {
    nativeSetName(address, ((com.alibaba.fastffi.FFIPointerImpl) name).address);
  }

  @FFINameAlias("set_name")
  public static native void nativeSetName(long ptr, long name0);
}
