package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "std::unordered_map<unsigned,uint64_t>",
    factory = StdUnorderedMap_cxx_0x4ac1da48Factory.class
)
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdUnorderedMap")
public class StdUnorderedMap_cxx_0x4ac1da48 extends FFIPointerImpl implements StdUnorderedMap<Integer, Long> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StdUnorderedMap_cxx_0x4ac1da48.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StdUnorderedMap_cxx_0x4ac1da48(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StdUnorderedMap_cxx_0x4ac1da48 that = (StdUnorderedMap_cxx_0x4ac1da48) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

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

  @CXXReference
  @CXXOperator("[]")
  public Long get(@CXXReference Integer key) {
    return new java.lang.Long(nativeGet(address, key));
  }

  @CXXReference
  @CXXOperator("[]")
  public static native long nativeGet(long ptr, int key0);

  @CXXOperator("[]")
  public void set(@CXXReference Integer key, @CXXReference Long value) {
    nativeSet(address, key, value);
  }

  @CXXOperator("[]")
  public static native void nativeSet(long ptr, int key0, long value1);

  public int size() {
    return nativeSize(address);
  }

  public static native int nativeSize(long ptr);

  public static native long nativeCreateFactory0();
}
