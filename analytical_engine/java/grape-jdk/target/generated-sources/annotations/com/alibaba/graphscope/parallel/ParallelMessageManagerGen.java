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
import com.alibaba.graphscope.fragment.ImmutableEdgecutFragment;
import java.lang.Long;

@FFITypeAlias("grape::ParallelMessageManager")
@CXXHead({
    "grape/parallel/parallel_message_manager.h",
    "core/fragment/arrow_projected_fragment.h",
    "grape/fragment/immutable_edgecut_fragment.h",
    "grape/graph/adj_list.h",
    "core/java/java_messages.h"
})
@FFIGen(
    library = "grape-jni"
)
@FFISynthetic("com.alibaba.graphscope.parallel.ParallelMessageManager")
public abstract interface ParallelMessageManagerGen extends ParallelMessageManager {
  @FFINameAlias("SendToFragment")
  default <MSG_T> void sendToFragment(int dstFid, @CXXReference MSG_T msg, int channelId) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendToFragment, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T, VDATA_T> void syncStateOnOuterVertexImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, int channel_id, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.syncStateOnOuterVertexImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T, VDATA_T> void syncStateOnOuterVertexArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, int channel_id, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.syncStateOnOuterVertexArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends ImmutableEdgecutFragment, VDATA_T> void syncStateOnOuterVertexImmutableNoMsg(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex, int channel_id,
      @FFISkip VDATA_T vdata) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.syncStateOnOuterVertexImmutableNoMsg, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SyncStateOnOuterVertex")
  default <FRAG_T extends ArrowProjectedFragment, VDATA_T> void syncStateOnOuterVertexArrowProjectedNoMsg(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex, int channel_id,
      @FFISkip VDATA_T vdata) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.syncStateOnOuterVertexArrowProjectedNoMsg, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughOEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T, VDATA_T> void sendMsgThroughOEdgesImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, int channel_id, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughOEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughOEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T, VDATA_T> void sendMsgThroughOEdgesArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, int channel_id, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughOEdgesArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T, VDATA_T> void sendMsgThroughEdgesImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, int channel_id, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T, VDATA_T> void sendMsgThroughEdgesArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, int channel_id, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughEdgesArrowProjected, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughIEdges")
  default <FRAG_T extends ImmutableEdgecutFragment, MSG_T, VDATA_T> void sendMsgThroughIEdgesImmutable(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, int channel_id, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughIEdgesImmutable, no template instantiation for the type arguments.");
  }

  @FFINameAlias("SendMsgThroughIEdges")
  default <FRAG_T extends ArrowProjectedFragment, MSG_T, VDATA_T> void sendMsgThroughIEdgesArrowProjected(
      @CXXReference FRAG_T frag,
      @CXXReference @FFITypeAlias("grape::Vertex<uint64_t>") Vertex<Long> vertex,
      @CXXReference MSG_T msg, int channel_id, @FFISkip VDATA_T unused) {
    throw new RuntimeException("Cannot call ParallelMessageManagerGen.sendMsgThroughIEdgesArrowProjected, no template instantiation for the type arguments.");
  }
}
