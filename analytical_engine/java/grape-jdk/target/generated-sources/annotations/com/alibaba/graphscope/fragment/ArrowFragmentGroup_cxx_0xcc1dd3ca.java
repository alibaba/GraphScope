package com.alibaba.graphscope.fragment;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphscope.stdcxx.StdUnorderedMap;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("vineyard::ArrowFragmentGroup")
@FFISynthetic("com.alibaba.graphscope.fragment.ArrowFragmentGroup")
public class ArrowFragmentGroup_cxx_0xcc1dd3ca extends FFIPointerImpl implements ArrowFragmentGroup {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ArrowFragmentGroup_cxx_0xcc1dd3ca.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ArrowFragmentGroup_cxx_0xcc1dd3ca(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ArrowFragmentGroup_cxx_0xcc1dd3ca that = (ArrowFragmentGroup_cxx_0xcc1dd3ca) o;
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

  @FFINameAlias("edge_label_num")
  public int edgeLabelNum() {
    return nativeEdgeLabelNum(address);
  }

  @FFINameAlias("edge_label_num")
  public static native int nativeEdgeLabelNum(long ptr);

  @FFINameAlias("FragmentLocations")
  @CXXReference
  @FFITypeAlias("std::unordered_map<unsigned,uint64_t>")
  public StdUnorderedMap<Integer, Long> fragmentLocations() {
    long ret$ = nativeFragmentLocations(address); return (new com.alibaba.graphscope.stdcxx.StdUnorderedMap_cxx_0x4ac1da48(ret$));
  }

  @FFINameAlias("FragmentLocations")
  @CXXReference
  @FFITypeAlias("std::unordered_map<unsigned,uint64_t>")
  public static native long nativeFragmentLocations(long ptr);

  @FFINameAlias("Fragments")
  @CXXReference
  @FFITypeAlias("std::unordered_map<unsigned,uint64_t>")
  public StdUnorderedMap<Integer, Long> fragments() {
    long ret$ = nativeFragments(address); return (new com.alibaba.graphscope.stdcxx.StdUnorderedMap_cxx_0x4ac1da48(ret$));
  }

  @FFINameAlias("Fragments")
  @CXXReference
  @FFITypeAlias("std::unordered_map<unsigned,uint64_t>")
  public static native long nativeFragments(long ptr);

  @FFINameAlias("total_frag_num")
  public int totalFragNum() {
    return nativeTotalFragNum(address);
  }

  @FFINameAlias("total_frag_num")
  public static native int nativeTotalFragNum(long ptr);

  @FFINameAlias("vertex_label_num")
  public int vertexLabelNum() {
    return nativeVertexLabelNum(address);
  }

  @FFINameAlias("vertex_label_num")
  public static native int nativeVertexLabelNum(long ptr);
}
