/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_APP_BASE_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_APP_BASE_H_

#include <string>
#include <utility>
#include <vector>

#include "grape/grape.h"
#include "grape/utils/iterator_pair.h"

#include "core/app/app_base.h"
#include "core/app/pregel/pregel_compute_context.h"
#include "core/app/pregel/pregel_context.h"

namespace gs {

/**
 * @brief PregelAppBase is implemented with PIE programming model. The pregel
 * program is driven by the PIE functions.
 * @tparam FRAG_T
 * @tparam VERTEX_PROGRAM_T
 * @tparam COMBINATOR_T
 */
template <typename FRAG_T, typename VERTEX_PROGRAM_T,
          typename COMBINATOR_T = void>
class PregelAppBase
    : public AppBase<
          FRAG_T,
          PregelContext<FRAG_T, PregelComputeContext<
                                    FRAG_T, typename VERTEX_PROGRAM_T::vd_t,
                                    typename VERTEX_PROGRAM_T::md_t>>>,
      public grape::Communicator {
  using vd_t = typename VERTEX_PROGRAM_T::vd_t;
  using md_t = typename VERTEX_PROGRAM_T::md_t;
  using pregel_compute_context_t = PregelComputeContext<FRAG_T, vd_t, md_t>;

  using app_t = PregelAppBase<FRAG_T, VERTEX_PROGRAM_T>;
  using pregel_context_t = PregelContext<FRAG_T, pregel_compute_context_t>;
  INSTALL_DEFAULT_WORKER(app_t, pregel_context_t, FRAG_T)

 public:
  explicit PregelAppBase(const VERTEX_PROGRAM_T& program = VERTEX_PROGRAM_T(),
                         const COMBINATOR_T& combinator = COMBINATOR_T())
      : program_(program), combinator_(combinator) {}

  using vid_t = typename fragment_t::vid_t;

  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, pregel_context_t& ctx,
             message_manager_t& messages) {
    // superstep is 0 in PEval
    ctx.compute_context_.enable_combine();

    PregelVertex<fragment_t, vd_t, md_t> pregel_vertex;
    pregel_vertex.set_fragment(&frag);
    pregel_vertex.set_compute_context(&ctx.compute_context_);

    grape::IteratorPair<md_t*> null_messages(nullptr, nullptr);
    auto inner_vertices = frag.InnerVertices();

    for (auto v : inner_vertices) {
      pregel_vertex.set_vertex(v);
      program_.Init(pregel_vertex, ctx.compute_context_);
    }

    for (auto v : inner_vertices) {
      pregel_vertex.set_vertex(v);
      program_.Compute(null_messages, pregel_vertex, ctx.compute_context_);
    }

    ctx.compute_context_.apply_combine(combinator_);
    ctx.compute_context_.before_comm();

    auto outer_vertices = frag.OuterVertices();
    for (auto v : outer_vertices) {
      auto& msgs = (ctx.compute_context_.messages_out())[v];
      assert(msgs.size() <= 1);
      if (!msgs.empty()) {
        messages.SyncStateOnOuterVertex<fragment_t, md_t>(frag, v, msgs[0]);
        msgs.clear();
      }
    }

    {
      // Sync Aggregator
      for (auto& pair : ctx.compute_context_.aggregators()) {
        grape::InArchive iarc;
        std::vector<grape::InArchive> oarcs;
        std::string name = pair.first;
        pair.second->Serialize(iarc);
        pair.second->Reset();
        AllGather(std::move(iarc), oarcs);
        pair.second->DeserializeAndAggregate(oarcs);
        pair.second->StartNewRound();
      }
    }

    ctx.compute_context_.clear_for_next_round();
    if (!ctx.compute_context_.all_halted()) {
      messages.ForceContinue();
    }
  }

  void IncEval(const fragment_t& frag, pregel_context_t& ctx,
               message_manager_t& messages) {
    ctx.compute_context_.inc_step();
    {
      // get message
      vertex_t v(0);
      md_t msg;
      while (messages.GetMessage<fragment_t, md_t>(frag, v, msg)) {
        assert(frag.IsInnerVertex(v));
        ctx.compute_context_.messages_in[v].emplace_back(std::move(msg));
      }
    }

    PregelVertex<fragment_t, vd_t, md_t> pregel_vertex;
    pregel_vertex.set_fragment(&frag);
    pregel_vertex.set_compute_context(&ctx.compute_context_);

    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      if (ctx.compute_context_.active(v)) {
        pregel_vertex.set_vertex(v);
        auto& cur_msgs = (ctx.compute_context_.messages_in())[v];
        program_.Compute(
            grape::IteratorPair<md_t*>(
                &cur_msgs[0],
                &cur_msgs[0] + static_cast<ptrdiff_t>(cur_msgs.size())),
            pregel_vertex, ctx.compute_context_);
      }
    }

    ctx.compute_context_.apply_combine(combinator_);
    ctx.compute_context_.before_comm();

    auto outer_vertices = frag.OuterVertices();
    for (auto v : outer_vertices) {
      auto& msgs = (ctx.compute_context_.messages_out())[v];
      assert(msgs.size() <= 1);
      if (!msgs.empty()) {
        messages.SyncStateOnOuterVertex<fragment_t, md_t>(frag, v, msgs[0]);
        msgs.clear();
      }
    }

    {
      // Sync Aggregator
      for (auto& pair : ctx.compute_context_.aggregators()) {
        grape::InArchive iarc;
        std::vector<grape::InArchive> oarcs;
        std::string name = pair.first;
        pair.second->Serialize(iarc);
        pair.second->Reset();
        AllGather(std::move(iarc), oarcs);
        pair.second->DeserializeAndAggregate(oarcs);
        pair.second->StartNewRound();
      }
    }

    ctx.compute_context_.clear_for_next_round();
    if (!ctx.compute_context_.all_halted()) {
      messages.ForceContinue();
    }
  }

 private:
  VERTEX_PROGRAM_T program_;
  COMBINATOR_T combinator_;
};
/**
 * @brief This class is a specialized PregelAppBase without a combinator.
 * @tparam FRAG_T
 * @tparam VERTEX_PROGRAM_T
 */
template <typename FRAG_T, typename VERTEX_PROGRAM_T>
class PregelAppBase<FRAG_T, VERTEX_PROGRAM_T, void>
    : public AppBase<
          FRAG_T,
          PregelContext<FRAG_T, PregelComputeContext<
                                    FRAG_T, typename VERTEX_PROGRAM_T::vd_t,
                                    typename VERTEX_PROGRAM_T::md_t>>>,
      public grape::Communicator {
  using vd_t = typename VERTEX_PROGRAM_T::vd_t;
  using md_t = typename VERTEX_PROGRAM_T::md_t;
  using app_t = PregelAppBase<FRAG_T, VERTEX_PROGRAM_T>;
  using pregel_compute_context_t = PregelComputeContext<FRAG_T, vd_t, md_t>;
  using pregel_context_t = PregelContext<FRAG_T, pregel_compute_context_t>;

  INSTALL_DEFAULT_WORKER(app_t, pregel_context_t, FRAG_T)

 public:
  explicit PregelAppBase(const VERTEX_PROGRAM_T& program = VERTEX_PROGRAM_T())
      : program_(program) {}

  using vid_t = typename fragment_t::vid_t;

  using vertex_t = typename fragment_t::vertex_t;

  void PEval(const fragment_t& frag, pregel_context_t& ctx,
             message_manager_t& messages) {
    // superstep is 0 in PEval

    PregelVertex<fragment_t, vd_t, md_t> pregel_vertex;
    pregel_vertex.set_fragment(&frag);
    pregel_vertex.set_compute_context(&ctx.compute_context_);

    grape::IteratorPair<md_t*> null_messages(nullptr, nullptr);

    auto inner_vertices = frag.InnerVertices();

    for (auto v : inner_vertices) {
      pregel_vertex.set_vertex(v);
      program_.Init(pregel_vertex, ctx.compute_context_);
    }

    for (auto v : inner_vertices) {
      pregel_vertex.set_vertex(v);
      program_.Compute(null_messages, pregel_vertex, ctx.compute_context_);
    }

    {
      // Sync Aggregator
      for (auto& pair : ctx.compute_context_.aggregators()) {
        grape::InArchive iarc;
        std::vector<grape::InArchive> oarcs;
        std::string name = pair.first;
        pair.second->Serialize(iarc);
        pair.second->Reset();
        AllGather(std::move(iarc), oarcs);
        pair.second->DeserializeAndAggregate(oarcs);
        pair.second->StartNewRound();
      }
    }

    ctx.compute_context_.clear_for_next_round();

    if (!ctx.compute_context_.all_halted()) {
      messages.ForceContinue();
    }
  }

  void IncEval(const fragment_t& frag, pregel_context_t& ctx,
               message_manager_t& messages) {
    ctx.compute_context_.inc_step();
    {
      // get message
      vertex_t v(0);
      md_t msg;
      while (messages.GetMessage<fragment_t, md_t>(frag, v, msg)) {
        assert(frag.IsInnerVertex(v));
        ctx.compute_context_.messages_in()[v].emplace_back(std::move(msg));
      }
    }

    PregelVertex<fragment_t, vd_t, md_t> pregel_vertex;
    pregel_vertex.set_fragment(&frag);
    pregel_vertex.set_compute_context(&ctx.compute_context_);

    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      if (ctx.compute_context_.active(v)) {
        pregel_vertex.set_vertex(v);
        auto& cur_msgs = (ctx.compute_context_.messages_in())[v];
        program_.Compute(
            grape::IteratorPair<md_t*>(
                &cur_msgs[0],
                &cur_msgs[0] + static_cast<ptrdiff_t>(cur_msgs.size())),
            pregel_vertex, ctx.compute_context_);
      }
    }

    {
      // Sync Aggregator
      for (auto& pair : ctx.compute_context_.aggregators()) {
        grape::InArchive iarc;
        std::vector<grape::InArchive> oarcs;
        std::string name = pair.first;
        pair.second->Serialize(iarc);
        pair.second->Reset();
        AllGather(std::move(iarc), oarcs);
        pair.second->DeserializeAndAggregate(oarcs);
        pair.second->StartNewRound();
      }
    }

    ctx.compute_context_.clear_for_next_round();

    if (!ctx.compute_context_.all_halted()) {
      messages.ForceContinue();
    }
  }

 private:
  VERTEX_PROGRAM_T program_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_PREGEL_APP_BASE_H_
