package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.graphx.VineyardClient;
import com.alibaba.graphscope.stdcxx.StdSharedPtr;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "gs::ArrowFragmentGroupGetter",
    factory = ArrowFragmentGroupGetter_cxx_0xfcfb2a7dFactory.class
)
@FFISynthetic("com.alibaba.graphscope.fragment.ArrowFragmentGroupGetter")
public class ArrowFragmentGroupGetter_cxx_0xfcfb2a7d extends FFIPointerImpl implements ArrowFragmentGroupGetter {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ArrowFragmentGroupGetter_cxx_0xfcfb2a7d.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ArrowFragmentGroupGetter_cxx_0xfcfb2a7d(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrowFragmentGroupGetter_cxx_0xfcfb2a7d that = (ArrowFragmentGroupGetter_cxx_0xfcfb2a7d) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  @FFINameAlias("Get")
  @CXXValue
  @FFITypeAlias("std::shared_ptr<vineyard::ArrowFragmentGroup>")
  public StdSharedPtr<ArrowFragmentGroup> get(@CXXReference VineyardClient client, long groupId) {
    long ret$ = nativeGet(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.StdSharedPtr_cxx_0xf86281af.SIZE), ((com.alibaba.fastffi.FFIPointerImpl) client).address, groupId); return (new com.alibaba.graphscope.stdcxx.StdSharedPtr_cxx_0xf86281af(ret$));
  }

  @FFINameAlias("Get")
  @CXXValue
  @FFITypeAlias("std::shared_ptr<vineyard::ArrowFragmentGroup>")
  public static native long nativeGet(long ptr, long rv_base, long client0, long groupId1);

  public static native long nativeCreateFactory0();
}
