package com.alibaba.graphscope.graphx;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.stdcxx.StdMap;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "vineyard::Client",
    factory = VineyardClient_cxx_0xb9ed250fFactory.class
)
@FFISynthetic("com.alibaba.graphscope.graphx.VineyardClient")
public class VineyardClient_cxx_0xb9ed250f extends FFIPointerImpl implements VineyardClient {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(VineyardClient_cxx_0xb9ed250f.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public VineyardClient_cxx_0xb9ed250f(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VineyardClient_cxx_0xb9ed250f that = (VineyardClient_cxx_0xb9ed250f) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("ClusterInfo")
  @CXXValue
  public V6dStatus clusterInfo(
      @CXXReference @FFITypeAlias("std::map<uint64_t,vineyard::json>") StdMap<Long, Json> meta) {
    long ret$ = nativeClusterInfo(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.graphx.V6dStatus_cxx_0xd5a80136.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) meta).address); return (new com.alibaba.graphscope.graphx.V6dStatus_cxx_0xd5a80136(ret$));
  }

  @FFINameAlias("ClusterInfo")
  @CXXValue
  public static native long nativeClusterInfo(long ptr, long rv_base, long meta0);

  @FFINameAlias("Connect")
  @CXXValue
  public V6dStatus connect(@FFIConst @CXXReference FFIByteString endPoint) {
    long ret$ = nativeConnect(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.graphx.V6dStatus_cxx_0xd5a80136.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) endPoint).address); return (new com.alibaba.graphscope.graphx.V6dStatus_cxx_0xd5a80136(ret$));
  }

  @FFINameAlias("Connect")
  @CXXValue
  public static native long nativeConnect(long ptr, long rv_base, long endPoint0);

  public static native long nativeCreateFactory0();
}
