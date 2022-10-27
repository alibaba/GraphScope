package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.graphx.Json;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "std::map<uint64_t,vineyard::json>",
    factory = StdMap_cxx_0x7fd2b116Factory.class
)
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdMap")
public class StdMap_cxx_0x7fd2b116 extends FFIPointerImpl implements StdMap<Long, Json> {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StdMap_cxx_0x7fd2b116.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StdMap_cxx_0x7fd2b116(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StdMap_cxx_0x7fd2b116 that = (StdMap_cxx_0x7fd2b116) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @CXXReference
  @CXXOperator("[]")
  public Json get(@CXXReference Long key) {
    long ret$ = nativeGet(address, key); return (new com.alibaba.graphscope.graphx.Json_cxx_0xf3be2b0c(ret$));
  }

  @CXXReference
  @CXXOperator("[]")
  public static native long nativeGet(long ptr, long key0);

  public static native long nativeCreateFactory0();
}
