package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIForeignType;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointerImpl;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeFactory;
import java.lang.Object;
import java.lang.String;
import java.lang.UnsatisfiedLinkError;

@FFIForeignType("grape::ParallelMessageManager")
@FFISynthetic("com.alibaba.graphscope.parallel.ParallelMessageManagerGen")
public class ParallelMessageManagerGen_cxx_0x5a835738 extends FFIPointerImpl implements ParallelMessageManagerGen {
  public static final int SIZE;

  public static final int HASH_SHIFT;

  static {
    try {
      System.loadLibrary("grape-jni");
    } catch (UnsatisfiedLinkError e) {
      System.load(FFITypeFactory.findNativeLibrary(ParallelMessageManagerGen_cxx_0x5a835738.class, "grape-jni"));
    }
  }
  static {
    SIZE = _elementSize$$$();
    assert SIZE > 0;
    HASH_SHIFT = 31 - Integer.numberOfLeadingZeros(1 + SIZE);
    assert HASH_SHIFT > 0;
  }

  public ParallelMessageManagerGen_cxx_0x5a835738(final long address) {
    super(address);
  }

  private static final native int _elementSize$$$();

  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ParallelMessageManagerGen_cxx_0x5a835738 that = (ParallelMessageManagerGen_cxx_0x5a835738) o;
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

  @FFINameAlias("GetMessageInBuffer")
  public boolean getMessageInBuffer(@CXXReference MessageInBuffer buf) {
    return nativeGetMessageInBuffer(address, ((com.alibaba.fastffi.FFIPointerImpl) buf).address);
  }

  @FFINameAlias("GetMessageInBuffer")
  public static native boolean nativeGetMessageInBuffer(long ptr, long buf0);

  @FFINameAlias("InitChannels")
  public void initChannels(int channel_num) {
    nativeInitChannels(address, channel_num);
  }

  @FFINameAlias("InitChannels")
  public static native void nativeInitChannels(long ptr, int channel_num0);
}
