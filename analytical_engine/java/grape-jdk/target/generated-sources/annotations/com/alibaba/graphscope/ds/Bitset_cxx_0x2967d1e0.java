package com.alibaba.graphscope.ds;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "grape::Bitset",
    factory = Bitset_cxx_0x2967d1e0Factory.class
)
@FFISynthetic("com.alibaba.graphscope.ds.Bitset")
public class Bitset_cxx_0x2967d1e0 extends FFIPointerImpl implements Bitset {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(Bitset_cxx_0x2967d1e0.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public Bitset_cxx_0x2967d1e0(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Bitset_cxx_0x2967d1e0 that = (Bitset_cxx_0x2967d1e0) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public void clear() {
    nativeClear(address);
  }

  public static native void nativeClear(long ptr);

  public long count() {
    return nativeCount(address);
  }

  public static native long nativeCount(long ptr);

  @CXXOperator("delete")
  public void delete() {
    nativeDelete(address);
  }

  @CXXOperator("delete")
  public static native void nativeDelete(long ptr);

  public boolean empty() {
    return nativeEmpty(address);
  }

  public static native boolean nativeEmpty(long ptr);

  @FFINameAlias("get_bit")
  public boolean getBit(long i) {
    return nativeGetBit(address, i);
  }

  @FFINameAlias("get_bit")
  public static native boolean nativeGetBit(long ptr, long i0);

  public long get_word(long i) {
    return nativeGet_word(address, i);
  }

  public static native long nativeGet_word(long ptr, long i0);

  public void init(long size) {
    nativeInit(address, size);
  }

  public static native void nativeInit(long ptr, long size0);

  @FFINameAlias("partial_empty")
  public boolean partialEmpty(long begin, long end) {
    return nativePartialEmpty(address, begin, end);
  }

  @FFINameAlias("partial_empty")
  public static native boolean nativePartialEmpty(long ptr, long begin0, long end1);

  public long partial_count(long begin, long end) {
    return nativePartial_count(address, begin, end);
  }

  public static native long nativePartial_count(long ptr, long begin0, long end1);

  @FFINameAlias("reset_bit")
  public void resetBit(long i) {
    nativeResetBit(address, i);
  }

  @FFINameAlias("reset_bit")
  public static native void nativeResetBit(long ptr, long i0);

  @FFINameAlias("reset_bit_with_ret")
  public boolean resetBitWithRet(long i) {
    return nativeResetBitWithRet(address, i);
  }

  @FFINameAlias("reset_bit_with_ret")
  public static native boolean nativeResetBitWithRet(long ptr, long i0);

  @FFINameAlias("set_bit")
  public void setBit(long i) {
    nativeSetBit(address, i);
  }

  @FFINameAlias("set_bit")
  public static native void nativeSetBit(long ptr, long i0);

  @FFINameAlias("set_bit_with_ret")
  public boolean setBitWithRet(long i) {
    return nativeSetBitWithRet(address, i);
  }

  @FFINameAlias("set_bit_with_ret")
  public static native boolean nativeSetBitWithRet(long ptr, long i0);

  public void swap(@CXXReference Bitset other) {
    nativeSwap(address, ((com.alibaba.fastffi.FFIPointerImpl) other).address);
  }

  public static native void nativeSwap(long ptr, long other0);

  public static native long nativeCreateFactory0();
}
