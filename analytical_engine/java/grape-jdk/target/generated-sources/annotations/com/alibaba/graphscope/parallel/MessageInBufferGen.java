package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowFragment;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import java.lang.Long;

@FFITypeAlias("grape::MessageInBuffer")
@CXXHead({
    "grape/parallel/message_in_buffer.h",
    "grape/fragment/immutable_edgecut_fragment.h",
    "core/fragment/arrow_projected_fragment.h",
    "core/java/type_alias.h",
    "core/java/java_messages.h"
})
@FFIGen(
    library = "grape-jni"
)
@FFISynthetic("com.alibaba.graphscope.parallel.MessageInBuffer")
public abstract interface MessageInBufferGen extends MessageInBuffer {
  @FFINameAlias("GetMessage")
  default <FRAG_T extends ArrowFragment, MSG_T, VDATA_T> boolean getMessage(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call MessageInBufferGen.getMessage, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T, VDATA_T> boolean getMessageArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call MessageInBufferGen.getMessageArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T, VDATA_T> boolean getMessageImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call MessageInBufferGen.getMessageImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <MSG_T> boolean getPureMessage(@CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call MessageInBufferGen.getPureMessage, no template instantiation for the type arguments.");
  }
}
