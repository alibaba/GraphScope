package com.alibaba.graphscope.communication;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXHeads;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIConst;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;

@CXXHeads({
    @CXXHead("grape/communication/communicator.h"),
    @CXXHead("core/java/java_messages.h")
})
@FFITypeAlias("grape::Communicator")
@FFIGen(
    library = "grape-jni"
)
@FFISynthetic("com.alibaba.graphscope.communication.FFICommunicator")
public abstract interface FFICommunicatorGen extends FFICommunicator {
  @FFINameAlias("Sum")
  default <MSG_T> void sum(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.sum, no template instantiation for the type arguments.");
  }

  @FFINameAlias("Min")
  default <MSG_T> void min(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.min, no template instantiation for the type arguments.");
  }

  @FFINameAlias("Max")
  default <MSG_T> void max(@FFIConst @CXXReference MSG_T msgIn, @CXXReference MSG_T msgOut) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.max, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendTo")
  default <MST_T> void sendTo(int dstFid, @FFIConst @CXXReference MST_T msgOut) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.sendTo, no template instantiation for the type arguments.");
  }

  @FFINameAlias("RecvFrom")
  default <MSG_T> void receiveFrom(int srcFid, @CXXReference MSG_T msgIn) {
    throw new RuntimeException("Cannot call FFICommunicatorGen.receiveFrom, no template instantiation for the type arguments.");
  }
}
