package com.alibaba.graphscope.parallel;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFISkip;
import com.alibaba.fastffi.FFISynthetic;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.graphscope.ds.Vertex;
import com.alibaba.graphscope.fragment.ArrowProjectedFragment;
import com.alibaba.graphscope.fragment.BaseGraphXFragment;
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import java.lang.Long;

@FFITypeAlias("grape::DefaultMessageManager")
@CXXHead({
    "grape/graph/adj_list.h",
    "grape/parallel/default_message_manager.h",
    "grape/fragment/immutable_edgecut_fragment.h",
    "core/fragment/arrow_projected_fragment.h",
    "core/java/java_messages.h",
    "core/java/graphx/graphx_fragment.h"
})
@FFIGen(
    library = "grape-jni"
)
@FFISynthetic("com.alibaba.graphscope.parallel.DefaultMessageManager")
public abstract interface DefaultMessageManagerGen extends DefaultMessageManager {
  @FFINameAlias("SendToFragment")
  default <MSG_T> void sendToFragment(int dst_fid, @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendToFragment, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <MSG_T> boolean getPureMessage(@CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.getPureMessage, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> boolean getMessageImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.getMessageImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("GetMessage")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T, SKIP_T> boolean getMessageArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, @FFISkip SKIP_T skip) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.getMessageArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void syncStateOnOuterVertexImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.syncStateOnOuterVertexImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T> void syncStateOnOuterVertexArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.syncStateOnOuterVertexArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends BaseGraphXFragment, MSG_T, SKIP_T> void syncStateOnOuterVertexGraphX(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, @FFISkip SKIP_T skip) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.syncStateOnOuterVertexGraphX, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughOEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughOEdgesImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughOEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughOEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughOEdgesArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughOEdgesArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughIEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughIEdgesImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughIEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughIEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughIEdgesArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughIEdgesArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T> void sendMsgThroughEdgesImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T> void sendMsgThroughEdgesArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg) {
    throw new RuntimeException("Cannot call DefaultMessageManagerGen.sendMsgThroughEdgesArrowProjected, no template instantiation for the type arguments.");
  }
}
