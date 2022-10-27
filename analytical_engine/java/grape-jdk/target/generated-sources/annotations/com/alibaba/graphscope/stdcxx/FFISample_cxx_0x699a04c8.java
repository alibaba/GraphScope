package com.alibaba.graphscope.stdcxx;

import com.alibaba.fastffi.CXXOperator;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIByteString;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIGetter;
import com.alibaba.fastffi.FFIMirrorDefinition;
import com.alibaba.fastffi.FFIMirrorFieldDefinition;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISetter;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.fastffi.FFIVector;
import java.lang.Byte;
import java.lang.Double;
import java.lang.Integer;
import java.lang.Long;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIMirrorDefinition(
    header = "jni_com_alibaba_graphscope_stdcxx_FFISample_cxx_0x699a04c8.h",
    name = "FFIMirrorSample",
    namespace = "sample",
    fields = {
        @FFIMirrorFieldDefinition(name = "doubleVectorField", foreignType = "std::vector<jdouble>", javaType = "com.alibaba.fastffi.FFIVector<java.lang.Double>"),
        @FFIMirrorFieldDefinition(name = "doubleVectorVectorField", foreignType = "std::vector<std::vector<jdouble>>", javaType = "com.alibaba.fastffi.FFIVector<com.alibaba.fastffi.FFIVector<java.lang.Double>>"),
        @FFIMirrorFieldDefinition(name = "intField", foreignType = "jint", javaType = "int"),
        @FFIMirrorFieldDefinition(name = "intVectorField", foreignType = "std::vector<jint>", javaType = "com.alibaba.fastffi.FFIVector<java.lang.Integer>"),
        @FFIMirrorFieldDefinition(name = "intVectorVectorField", foreignType = "std::vector<std::vector<jint>>", javaType = "com.alibaba.fastffi.FFIVector<com.alibaba.fastffi.FFIVector<java.lang.Integer>>"),
        @FFIMirrorFieldDefinition(name = "longVectorField", foreignType = "std::vector<jlong>", javaType = "com.alibaba.fastffi.FFIVector<java.lang.Long>"),
        @FFIMirrorFieldDefinition(name = "longVectorVectorField", foreignType = "std::vector<std::vector<jlong>>", javaType = "com.alibaba.fastffi.FFIVector<com.alibaba.fastffi.FFIVector<java.lang.Long>>"),
        @FFIMirrorFieldDefinition(name = "stringField", foreignType = "std::string", javaType = "com.alibaba.fastffi.FFIByteString"),
        @FFIMirrorFieldDefinition(name = "vectorBytes", foreignType = "std::vector<jbyte>", javaType = "com.alibaba.fastffi.FFIVector<java.lang.Byte>")
    }
)
@FFIForeignType(
    value = "sample::FFIMirrorSample",
    factory = FFISample_cxx_0x699a04c8Factory.class
)
@FFISynthetic("com.alibaba.graphscope.stdcxx.FFISample")
public class FFISample_cxx_0x699a04c8 extends FFIPointerImpl implements FFISample {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(FFISample_cxx_0x699a04c8.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  private int intProperty;

  public FFISample_cxx_0x699a04c8(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FFISample_cxx_0x699a04c8 that = (FFISample_cxx_0x699a04c8) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return javaHashCode();
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

  @FFIGetter
  @CXXReference
  public FFIVector<Double> doubleVectorField() {
    long ret$ = nativeDoubleVectorField(address); return (new com.alibaba.fastffi.impl.CXXStdVector_cxx_0x3356ee39(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeDoubleVectorField(long ptr);

  @FFIGetter
  @CXXReference
  public FFIVector<FFIVector<Double>> doubleVectorVectorField() {
    long ret$ = nativeDoubleVectorVectorField(address); return (new com.alibaba.fastffi.impl.CXXStdVector_cxx_0x33d2198f(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeDoubleVectorVectorField(long ptr);

  @FFIGetter
  public int intField() {
    return nativeIntField0(address);
  }

  @FFIGetter
  public static native int nativeIntField0(long ptr);

  @FFISetter
  public void intField(int value) {
    nativeIntField1(address, value);
  }

  @FFISetter
  public static native void nativeIntField1(long ptr, int value0);

  public int intProperty() {
    return this.intProperty;
  }

  public void intProperty(int value) {
    this.intProperty = value;
  }

  @FFIGetter
  @CXXReference
  public FFIVector<Integer> intVectorField() {
    long ret$ = nativeIntVectorField(address); return (new com.alibaba.fastffi.impl.CXXStdVector_cxx_0xb9247603(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeIntVectorField(long ptr);

  @FFIGetter
  @CXXReference
  public FFIVector<FFIVector<Integer>> intVectorVectorField() {
    long ret$ = nativeIntVectorVectorField(address); return (new com.alibaba.fastffi.impl.CXXStdVector_cxx_0xbb6af271(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeIntVectorVectorField(long ptr);

  @FFIGetter
  @CXXReference
  public FFIVector<Long> longVectorField() {
    long ret$ = nativeLongVectorField(address); return (new com.alibaba.fastffi.impl.CXXStdVector_cxx_0x6b94f3ee(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeLongVectorField(long ptr);

  @FFIGetter
  @CXXReference
  public FFIVector<FFIVector<Long>> longVectorVectorField() {
    long ret$ = nativeLongVectorVectorField(address); return (new com.alibaba.fastffi.impl.CXXStdVector_cxx_0xb71ddcfa(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeLongVectorVectorField(long ptr);

  @FFIGetter
  @CXXReference
  public FFIByteString stringField() {
    long ret$ = nativeStringField(address); return (new com.alibaba.fastffi.impl.CXXStdString_cxx_0xcec1e274(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeStringField(long ptr);

  @FFIGetter
  @CXXReference
  public FFIVector<Byte> vectorBytes() {
    long ret$ = nativeVectorBytes(address); return (new com.alibaba.fastffi.impl.CXXStdVector_cxx_0x6b0caae2(ret$));
  }

  @FFIGetter
  @CXXReference
  public static native long nativeVectorBytes(long ptr);

  public static native long nativeCreateFactory0();

  public static native long nativeCreateStackFactory1(long rv_base);
}
