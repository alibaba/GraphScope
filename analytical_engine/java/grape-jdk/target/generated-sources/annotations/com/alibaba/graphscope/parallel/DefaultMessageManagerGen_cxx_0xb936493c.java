package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("grape::DefaultMessageManager")
@FFISynthetic("com.alibaba.graphscope.parallel.DefaultMessageManagerGen")
public class DefaultMessageManagerGen_cxx_0xb936493c extends FFIPointerImpl implements DefaultMessageManagerGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(DefaultMessageManagerGen_cxx_0xb936493c.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public DefaultMessageManagerGen_cxx_0xb936493c(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DefaultMessageManagerGen_cxx_0xb936493c that = (DefaultMessageManagerGen_cxx_0xb936493c) o;
    return this.address == that.address;
  }

  public int hashCode() {
    return (int) (address >> HASH_SHIFT);
  }

  public String toString() {
    return getClass().getName() + "@" + Long.toHexString(address);
  }

  public void Finalize() {
    native_Finalize(address);
  }

  public static native void native_Finalize(long ptr);

  public void FinishARound() {
    native_FinishARound(address);
  }

  public static native void native_FinishARound(long ptr);

  public void ForceContinue() {
    native_ForceContinue(address);
  }

  public static native void native_ForceContinue(long ptr);

  public long GetMsgSize() {
    return native_GetMsgSize(address);
  }

  public static native long native_GetMsgSize(long ptr);

  public void Start() {
    native_Start(address);
  }

  public static native void native_Start(long ptr);

  public void StartARound() {
    native_StartARound(address);
  }

  public static native void native_StartARound(long ptr);

  public boolean ToTerminate() {
    return native_ToTerminate(address);
  }

  public static native boolean native_ToTerminate(long ptr);
}
