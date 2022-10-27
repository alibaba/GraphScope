package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("unsigned")
@FFISynthetic("com.alibaba.graphscope.ds.FidPointer")
public class FidPointer_cxx_0xff0c66f5 extends FFIPointerImpl implements FidPointer {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(FidPointer_cxx_0xff0c66f5.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public FidPointer_cxx_0xff0c66f5(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FidPointer_cxx_0xff0c66f5 that = (FidPointer_cxx_0xff0c66f5) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public FidPointer add(long arg0) {
    return new FidPointer_cxx_0xff0c66f5(this.address + arg0);
  }

  public void addV(long arg0) {
    this.address += arg0;
  }

  public long elementSize() {
    return SIZE;
  }

  public FidPointer moveTo(long arg0) {
    return new FidPointer_cxx_0xff0c66f5(arg0);
  }

  public void moveToV(long arg0) {
    this.address = arg0;
  }

  public void setAddress(long arg0) {
    this.address = arg0;
  }
}
