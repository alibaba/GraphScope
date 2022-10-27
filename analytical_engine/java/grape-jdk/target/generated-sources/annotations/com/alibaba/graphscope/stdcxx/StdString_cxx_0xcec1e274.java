package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType(
    value = "std::string",
    factory = StdString_cxx_0xcec1e274Factory.class
)
@FFISynthetic("com.alibaba.graphscope.stdcxx.StdString")
public class StdString_cxx_0xcec1e274 extends FFIPointerImpl implements StdString {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(StdString_cxx_0xcec1e274.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public StdString_cxx_0xcec1e274(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StdString_cxx_0xcec1e274 that = (StdString_cxx_0xcec1e274) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return toJavaString();
  }

  @CXXReference
  public StdString append(@CXXReference StdString rhs) {
    long ret$ = nativeAppend(address, ((com.alibaba.fastffi.FFIPointerImpl) rhs).address); return (new com.alibaba.graphscope.stdcxx.StdString_cxx_0xcec1e274(ret$));
  }

  @CXXReference
  public static native long nativeAppend(long ptr, long rhs0);

  public byte at(long index) {
    return nativeAt(address, index);
  }

  public static native byte nativeAt(long ptr, long index0);

  @CXXValue
  public StdString.Iterator begin() {
    long ret$ = nativeBegin(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.StdString_Iterator_cxx_0xa5913122.SIZE)); return (new com.alibaba.graphscope.stdcxx.StdString_Iterator_cxx_0xa5913122(ret$));
  }

  @CXXValue
  public static native long nativeBegin(long ptr, long rv_base);

  public long c_str() {
    return nativeC_str(address);
  }

  public static native long nativeC_str(long ptr);

  public void clear() {
    nativeClear(address);
  }

  public static native void nativeClear(long ptr);

  public int compare(@CXXReference StdString str) {
    return nativeCompare(address, ((com.alibaba.fastffi.FFIPointerImpl) str).address);
  }

  public static native int nativeCompare(long ptr, long str0);

  public long data() {
    return nativeData(address);
  }

  public static native long nativeData(long ptr);

  @CXXOperator("delete")
  public void delete() {
    nativeDelete(address);
  }

  @CXXOperator("delete")
  public static native void nativeDelete(long ptr);

  @CXXValue
  public StdString.Iterator end() {
    long ret$ = nativeEnd(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.StdString_Iterator_cxx_0xa5913122.SIZE)); return (new com.alibaba.graphscope.stdcxx.StdString_Iterator_cxx_0xa5913122(ret$));
  }

  @CXXValue
  public static native long nativeEnd(long ptr, long rv_base);

  public long find(@CXXReference StdString str, long pos) {
    return nativeFind1(address, ((com.alibaba.fastffi.FFIPointerImpl) str).address, pos);
  }

  public static native long nativeFind1(long ptr, long str0, long pos1);

  public long find(byte c, long pos) {
    return nativeFind3(address, c, pos);
  }

  public static native long nativeFind3(long ptr, byte c0, long pos1);

  public long find_first_of(@CXXReference StdString str, long pos) {
    return nativeFind_first_of1(address, ((com.alibaba.fastffi.FFIPointerImpl) str).address, pos);
  }

  public static native long nativeFind_first_of1(long ptr, long str0, long pos1);

  public long find_first_of(byte c, long pos) {
    return nativeFind_first_of3(address, c, pos);
  }

  public static native long nativeFind_first_of3(long ptr, byte c0, long pos1);

  public long find_last_of(@CXXReference StdString str, long pos) {
    return nativeFind_last_of1(address, ((com.alibaba.fastffi.FFIPointerImpl) str).address, pos);
  }

  public static native long nativeFind_last_of1(long ptr, long str0, long pos1);

  public long find_last_of(byte c, long pos) {
    return nativeFind_last_of3(address, c, pos);
  }

  public static native long nativeFind_last_of3(long ptr, byte c0, long pos1);

  public void push_back(byte c) {
    nativePush_back(address, c);
  }

  public static native void nativePush_back(long ptr, byte c0);

  public void resize(long size) {
    nativeResize(address, size);
  }

  public static native void nativeResize(long ptr, long size0);

  public long size() {
    return nativeSize(address);
  }

  public static native long nativeSize(long ptr);

  @CXXValue
  public StdString substr(long pos, long len) {
    long ret$ = nativeSubstr1(address, com.alibaba.fastffi.CXXValueScope.allocate(com.alibaba.graphscope.stdcxx.StdString_cxx_0xcec1e274.SIZE), pos, len); return (new com.alibaba.graphscope.stdcxx.StdString_cxx_0xcec1e274(ret$));
  }

  @CXXValue
  public static native long nativeSubstr1(long ptr, long rv_base, long pos0, long len1);

  public static native long nativeCreateFactory0();

  public static native long nativeCreateFactory1(long string0);
}
