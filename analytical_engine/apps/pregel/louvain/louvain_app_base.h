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

#ifndef ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_APP_BASE_H_
#define ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_APP_BASE_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "grape/grape.h"
#include "grape/utils/iterator_pair.h"

#include "core/app/app_base.h"
#include "core/app/pregel/pregel_compute_context.h"

#include "apps/pregel/louvain/auxiliary.h"
#include "apps/pregel/louvain/louvain.h"
#include "apps/pregel/louvain/louvain_context.h"
#include "apps/pregel/louvain/louvain_vertex.h"

namespace gs {

/**
 * @brief This class is a specialized PregelAppBase for louvain.
 * @param FRAG_T
 * @param VERTEX_PROGRAM_T
 */
template <typename FRAG_T, typename VERTEX_PROGRAM_T = PregelLouvain<FRAG_T>>
class LouvainAppBase
    : public grape::ParallelAppBase<
          FRAG_T,
          LouvainContext<FRAG_T, PregelComputeContext<
                                     FRAG_T, typename VERTEX_PROGRAM_T::vd_t,
                                     typename VERTEX_PROGRAM_T::md_t>>>,
      public grape::ParallelEngine,
      public grape::Communicator {
 public:
  using fragment_t = FRAG_T;
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using app_t = LouvainAppBase<FRAG_T, VERTEX_PROGRAM_T>;
  using vertex_program_t = VERTEX_PROGRAM_T;
  using vd_t = typename vertex_program_t::vd_t;
  using md_t = typename vertex_program_t::md_t;
  using pregel_compute_context_t = PregelComputeContext<FRAG_T, vd_t, md_t>;
  using context_t = LouvainContext<FRAG_T, pregel_compute_context_t>;
  using message_manager_t = grape::ParallelMessageManager;
  using worker_t = grape::ParallelWorker<app_t>;

  virtual ~LouvainAppBase<FRAG_T, VERTEX_PROGRAM_T>() {}

  static std::shared_ptr<worker_t> CreateWorker(std::shared_ptr<app_t> app,
                                                std::shared_ptr<FRAG_T> frag) {
    return std::shared_ptr<worker_t>(new worker_t(app, frag));
  }

  explicit LouvainAppBase(const vertex_program_t& program = vertex_program_t())
      : program_(program) {}

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    // superstep is 0 in PEval
    uint32_t thrd_num = thread_num();
    messages.InitChannels(thrd_num);

    // register the aggregators
    ctx.compute_context().register_aggregator(
        change_aggregator, PregelAggregatorType::kInt64SumAggregator);
    ctx.compute_context().register_aggregator(
        edge_weight_aggregator, PregelAggregatorType::kDoubleSumAggregator);
    ctx.compute_context().register_aggregator(
        actual_quality_aggregator, PregelAggregatorType::kDoubleSumAggregator);
    ctx.ClearLocalAggregateValues(thrd_num);

    auto inner_vertices = frag.InnerVertices();
    ForEach(inner_vertices, [&frag, &ctx, this](int tid, vertex_t v) {
      LouvainVertex<fragment_t, vd_t, md_t> pregel_vertex;
      pregel_vertex.set_context(&ctx);
      pregel_vertex.set_fragment(&frag);
      pregel_vertex.set_compute_context(&ctx.compute_context());
      pregel_vertex.set_vertex(v);
      pregel_vertex.set_tid(tid);
      this->program_.Init(pregel_vertex, ctx.compute_context());
    });

    grape::IteratorPair<md_t*> null_messages(nullptr, nullptr);
    ForEach(inner_vertices,
            [&null_messages, &frag, &ctx, this](int tid, vertex_t v) {
              LouvainVertex<fragment_t, vd_t, md_t> pregel_vertex;
              pregel_vertex.set_context(&ctx);
              pregel_vertex.set_fragment(&frag);
              pregel_vertex.set_compute_context(&ctx.compute_context());
              pregel_vertex.set_vertex(v);
              pregel_vertex.set_tid(tid);
              this->program_.Compute(null_messages, pregel_vertex,
                                     ctx.compute_context());
            });

    {
      // sync aggregator
      ctx.compute_context().aggregate(change_aggregator,
                                      ctx.GetLocalChangeSum());
      ctx.compute_context().aggregate(edge_weight_aggregator,
                                      ctx.GetLocalEdgeWeightSum());
      ctx.compute_context().aggregate(actual_quality_aggregator,
                                      ctx.GetLocalQualitySum());
      for (auto& pair : ctx.compute_context().aggregators()) {
        grape::InArchive iarc;
        std::vector<grape::InArchive> oarcs;
        std::string name = pair.first;
        pair.second->Serialize(iarc);
        pair.second->Reset();
        AllGather(std::move(iarc), oarcs);
        pair.second->DeserializeAndAggregate(oarcs);
        pair.second->StartNewRound();
      }
      ctx.ClearLocalAggregateValues(thrd_num);
    }

    ctx.compute_context().clear_for_next_round();

    if (!ctx.compute_context().all_halted()) {
      messages.ForceContinue();
    }
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    ctx.compute_context().inc_step();
    uint32_t thrd_num = thread_num();

    auto inner_vertices = frag.InnerVertices();

    int current_super_step = ctx.compute_context().superstep();
    // the minor step in phase 1
    int current_minor_step = current_super_step % 3;
    // the current iteration, two iterations make a full pass.
    int current_iteration = current_super_step / 3;

    VLOG(1) << "current super step: " << current_super_step
            << " current minor step: " << current_minor_step
            << " current iteration: " << current_iteration;

    if (current_super_step == terminate_step) {
      // get result messages and terminate
      messages.ParallelProcess<std::pair<vid_t, oid_t>>(
          thrd_num, [&frag, &ctx](int tid, std::pair<vid_t, oid_t> const& msg) {
            vertex_t v;
            frag.InnerVertexGid2Vertex(msg.first, v);
            ctx.compute_context().vertex_data()[v] = msg.second;
          });
      return;  // the whole louvain terminate.
    } else {
      // get computation messages
      std::vector<std::vector<std::vector<md_t>>> buffer(
          thrd_num, std::vector<std::vector<md_t>>(thrd_num));
      messages.ParallelProcess<md_t>(
          thrd_num, [&thrd_num, &buffer](int tid, md_t const& msg) {
            buffer[tid][msg.dst_id % thrd_num].emplace_back(std::move(msg));
          });
      {
        std::vector<std::thread> threads(thrd_num);
        for (uint32_t tid = 0; tid < thrd_num; ++tid) {
          threads[tid] = std::thread(
              [&frag, &ctx, &thrd_num, &buffer](uint32_t tid) {
                for (uint32_t index = 0; index < thrd_num; ++index) {
                  for (auto const& msg : buffer[index][tid]) {
                    vertex_t v;
                    frag.InnerVertexGid2Vertex(msg.dst_id, v);
                    ctx.compute_context().messages_in()[v].emplace_back(
                        std::move(msg));
                    ctx.compute_context().activate(v);
                  }
                }
              },
              tid);
        }
        for (uint32_t tid = 0; tid < thrd_num; ++tid) {
          threads[tid].join();
        }
      }
    }

    if (current_minor_step == phase_one_minor_step_1 && current_iteration > 0 &&
        current_iteration % 2 == 0) {
      // aggreate total change
      int64_t total_change =
          ctx.compute_context().template get_aggregated_value<int64_t>(
              change_aggregator);
      ctx.change_history().push_back(total_change);
      // check whether to halt phase-1
      bool to_halt = decide_to_halt(ctx.change_history(), ctx.min_progress(),
                                    ctx.progress_tries());
      ctx.set_halt(to_halt);
      if (ctx.halt()) {
        // if halt, first aggregate actual quality in Compute of vertices and
        // then start phase 2 in next super step.
        VLOG(1) << "super step " << current_super_step << " decided to halt.";
        messages.ForceContinue();
      }
      VLOG(1) << "[INFO]: superstep: " << current_super_step
              << " pass: " << current_iteration / 2
              << " total change: " << total_change;
    } else if (ctx.halt()) {
      // after decide_to_halt and aggregate actual quality in previous super
      // step, here we check terminate computaion or start phase 2.
      double actual_quality =
          ctx.compute_context().template get_aggregated_value<double>(
              actual_quality_aggregator);
      // after one pass if already decided halt, that means the pass yield no
      // changes, so we halt computation.
      if (current_super_step <= 14 || actual_quality <= ctx.prev_quality()) {
        // turn to sync community result
        ctx.compute_context().set_superstep(sync_result_step);
        syncCommunity(frag, ctx, messages);
        messages.ForceContinue();

        LOG(INFO) << "computation complete, ACTUAL QUALITY: " << actual_quality;
        return;
      } else if (ctx.compute_context().superstep() > 0) {
        // just halt phase 1 start phase 2.
        VLOG(1) << "super step: " << current_super_step
                << " decided to halt, ACTUAL QUALITY: " << actual_quality
                << " previous QUALITY: " << ctx.prev_quality();

        // start phase 2 to compress community.
        ctx.compute_context().set_superstep(phase_two_start_step);
        ctx.set_prev_quality(actual_quality);
        ctx.change_history().clear();
        ctx.set_halt(false);
      }
    }

    // At the start of each pass, every alive node need to their
    // communities node info to neighbors, so we active the node.
    if (ctx.compute_context().superstep() == phase_two_start_step) {
      ForEach(inner_vertices, [&ctx](int tid, vertex_t v) {
        if (ctx.GetVertexState(v).is_alived_community) {
          ctx.compute_context().activate(v);
        }
      });
    }

    ForEach(inner_vertices, [&frag, &ctx, this](int tid, vertex_t v) {
      if (ctx.compute_context().active(v)) {
        LouvainVertex<fragment_t, vd_t, md_t> pregel_vertex;
        pregel_vertex.set_context(&ctx);
        pregel_vertex.set_fragment(&frag);
        pregel_vertex.set_compute_context(&ctx.compute_context());
        pregel_vertex.set_vertex(v);
        pregel_vertex.set_tid(tid);
        auto& cur_msgs = (ctx.compute_context().messages_in())[v];
        this->program_.Compute(
            grape::IteratorPair<md_t*>(
                &cur_msgs[0],
                &cur_msgs[0] + static_cast<ptrdiff_t>(cur_msgs.size())),
            pregel_vertex, ctx.compute_context());
      } else if (ctx.compute_context().superstep() == compress_community_step) {
        ctx.GetVertexState(v).is_alived_community = false;
      }
    });

    {
      // sync aggregator
      ctx.compute_context().aggregate(change_aggregator,
                                      ctx.GetLocalChangeSum());
      ctx.compute_context().aggregate(edge_weight_aggregator,
                                      ctx.GetLocalEdgeWeightSum());
      ctx.compute_context().aggregate(actual_quality_aggregator,
                                      ctx.GetLocalQualitySum());
      for (auto& pair : ctx.compute_context().aggregators()) {
        grape::InArchive iarc;
        std::vector<grape::InArchive> oarcs;
        std::string name = pair.first;
        pair.second->Serialize(iarc);
        pair.second->Reset();
        AllGather(std::move(iarc), oarcs);
        pair.second->DeserializeAndAggregate(oarcs);
        pair.second->StartNewRound();
      }
      ctx.ClearLocalAggregateValues(thrd_num);
    }

    ctx.compute_context().clear_for_next_round();

    if (!ctx.compute_context().all_halted()) {
      messages.ForceContinue();
    }
  }

 private:
  // sync community id from community hub to community members.
  void syncCommunity(const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages) {
    auto& vid_parser = ctx.compute_context().vid_parser();
    auto& comm_result = ctx.compute_context().vertex_data();
    auto inner_vertices = frag.InnerVertices();
    ForEach(inner_vertices, [&frag, &ctx, &messages, &comm_result, &vid_parser](
                                int tid, vertex_t v) {
      const auto& member_list = ctx.vertex_state()[v].nodes_in_community;
      if (!member_list.empty()) {
        auto community_id = frag.Gid2Oid(member_list.front());
        // send community id to members
        for (const auto& member_gid : member_list) {
          auto fid = vid_parser.GetFid(member_gid);
          vertex_t member_v;
          if (fid == frag.fid()) {
            frag.InnerVertexGid2Vertex(member_gid, member_v);
            comm_result[member_v] = community_id;
          } else {
            messages.Channels()[tid].SendToFragment(
                fid, std::pair<vid_t, oid_t>(member_gid, community_id));
          }
        }
      }
    });
  }

 private:
  vertex_program_t program_;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_PREGEL_LOUVAIN_LOUVAIN_APP_BASE_H_
