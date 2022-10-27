package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXHeads;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import java.lang.Long;

@CXXHeads({
    @CXXHead({"core/parallel/property_message_manager.h", "vineyard/graph/fragment/arrow_fragment.h", "core/java/java_messages.h", "core/java/type_alias.h"}),
    @CXXHead("cstdint")
})
@FFITypeAlias("gs::PropertyMessageManager")
@FFIGen(
    library = "grape-jni"
)
@FFISynthetic("com.alibaba.graphscope.parallel.PropertyMessageManager")
public abstract interface PropertyMessageManagerGen extends PropertyMessageManager {
  @FFINameAlias("SendMsgThroughIEdges")
  default <FRAG_T, MSG_T> void sendMsgThroughIEdges(@CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex, int eLabelId,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call PropertyMessageManagerGen.sendMsgThroughIEdges, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughOEdges")
  default <FRAG_T, MSG_T> void sendMsgThroughOEdges(@CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex, int eLabelId,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call PropertyMessageManagerGen.sendMsgThroughOEdges, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughEdges")
  default <FRAG_T, MSG_T> void sendMsgThroughEdges(@CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex, int eLabelId,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call PropertyMessageManagerGen.sendMsgThroughEdges, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T, MSG_T> void syncStateOnOuterVertex(@CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call PropertyMessageManagerGen.syncStateOnOuterVertex, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <FRAG_T, MSG_T> boolean getMessage(@CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call PropertyMessageManagerGen.getMessage, no template instantiation for the type arguments.");
  }
}
