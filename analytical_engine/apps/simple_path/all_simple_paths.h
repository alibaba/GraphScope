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
Author: Ma JingYuan<nn9902@qq.com>
*/

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_H_

#include <set>
#include <tuple>
#include <utility>
#include <vector>

#include "simple_path/all_simple_paths_context.h"

#include "core/app/app_base.h"
#include "core/worker/default_worker.h"

namespace gs {

template <typename FRAG_T>
class AllSimplePaths : public AppBase<FRAG_T, AllSimplePathsContext<FRAG_T>>,
                       public grape::Communicator {
 public:
  INSTALL_DEFAULT_WORKER(AllSimplePaths<FRAG_T>, AllSimplePathsContext<FRAG_T>,
                         FRAG_T)
  static constexpr grape::MessageStrategy message_strategy =
      grape::MessageStrategy::kAlongOutgoingEdgeToOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vertex_t source;
    vid_t source_gid;
    bool empty_flag = false;
    // Source does not exist.
    if (!frag.Oid2Gid(ctx.source_id, source_gid)) {
      empty_flag = true;
      ctx.nodes_not_found = true;
    }
    // nodes_not_found = true, return 2 as NodeNotFound.
    if (ctx.nodes_not_found == true && frag.fid() == 0) {
      std::vector<typename fragment_t::oid_t> data;
      std::vector<size_t> shape{1};
      data.push_back(2);
      ctx.assign(data, shape);
      return;
    }
    ctx.soucre_fid = source_gid >> ctx.fid_offset;
    if (ctx.targets.count(source_gid) || ctx.cutoff < 1) {
      empty_flag = true;
    }
    // empty_flag == true, return 1 as empty path.
    if (empty_flag == true && frag.fid() == ctx.soucre_fid) {
      std::vector<typename fragment_t::oid_t> data;
      std::vector<size_t> shape{1};
      data.push_back(1);
      ctx.assign(data, shape);
      return;
    } else if (empty_flag == true || ctx.nodes_not_found == true) {
      return;
    }
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);
    // If native_source resize edge_map, else send frag innervertices num to
    // source frag.
    if (native_source) {
      ctx.source_flag = true;
      size_t total_vertex_num = frag.GetTotalVerticesNum();
      ctx.edge_map.resize(total_vertex_num);
    } else {
      vid_t in_num = frag.GetInnerVerticesNum();
      fid_t fid = frag.fid();
      std::tuple<std::pair<vid_t, vid_t>, bool, bool> msg =
          std::make_tuple(std::make_pair((vid_t) fid, in_num), false, true);
      messages.SendToFragment(ctx.soucre_fid, msg);
    }
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    int frag_finish_counter = 0;
    int init_counter = 0;
    std::tuple<std::pair<vid_t, vid_t>, bool, bool> msg;

    while (messages.GetMessage(msg)) {
      vid_t gid = std::get<0>(msg).first;
      vid_t msg2 = std::get<0>(msg).second;
      // msg[2] == true means init frag_vertex_num, msg[1] == false means new
      // vertex to deal with, msg[1] == true means update edge_map.
      if (std::get<2>(msg) == true) {
        init_counter++;
        ctx.frag_vertex_num[gid] = msg2;
        if (init_counter == static_cast<int>(frag.fnum()) - 1) {
          vertex_t source;
          frag.GetInnerVertex(ctx.source_id, source);
          vertexProcess(source, frag, ctx, messages, 0);
          vid_t s_gid;
          frag.Oid2Gid(ctx.source_id, s_gid);
          ctx.visit.insert(s_gid);
          messages.ForceContinue();
          frag_finish_counter++;
        }
      } else if (std::get<1>(msg) == false) {
        if (ctx.visit.count(gid) == 0) {
          ctx.visit.insert(gid);
          ctx.next_level_inner.push(std::make_pair(gid, msg2));
        }
      } else if (ctx.source_flag == true && std::get<1>(msg) == true) {
        int a = findVertexGlobalIndex(ctx, gid);
        int b = findVertexGlobalIndex(ctx, msg2);
        ctx.edge_map[a].push_back(b);
      }
    }
    ctx.curr_level_inner.swap(ctx.next_level_inner);
    while (!ctx.curr_level_inner.empty()) {
      vid_t gid = ctx.curr_level_inner.front().first;
      int depth = ctx.curr_level_inner.front().second;
      ctx.curr_level_inner.pop();
      vertex_t v;
      frag.Gid2Vertex(gid, v);
      if (depth <= ctx.cutoff) {
        bool ret = vertexProcess(v, frag, ctx, messages, depth);
        if (ret == true)
          frag_finish_counter++;
      }
    }
    if (!ctx.next_level_inner.empty() || frag_finish_counter > 0)
      messages.ForceContinue();
    Sum(frag_finish_counter, ctx.frag_finish_counter);
    if (ctx.frag_finish_counter == 0 && frag.fid() == ctx.soucre_fid) {
      writeToCtx(frag, ctx);
    }
  }

 private:
  bool vertexProcess(vertex_t v, const fragment_t& frag, context_t& ctx,
                     message_manager_t& messages, int depth) {
    bool ret = false;
    std::set<vid_t> pvisit;
    vid_t gid = frag.Vertex2Gid(v);
    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      vertex_t u = e.get_neighbor();
      vid_t u_gid = frag.Vertex2Gid(u);
      if (pvisit.count(u_gid) == 0) {
        pvisit.insert(u_gid);
        if (ctx.source_flag == false) {
          std::tuple<std::pair<vid_t, vid_t>, bool, bool> msg =
              std::make_tuple(std::make_pair(gid, u_gid), true, false);
          messages.SendToFragment(ctx.soucre_fid, msg);
          ret = true;
        } else {
          int a = findVertexGlobalIndex(ctx, gid);
          int b = findVertexGlobalIndex(ctx, u_gid);
          ctx.edge_map[a].push_back(b);
        }
        if (!frag.IsOuterVertex(u)) {
          if (ctx.visit.count(u_gid) == 0) {
            ctx.visit.insert(u_gid);
            ctx.next_level_inner.push(std::make_pair(u_gid, depth + 1));
          }
        } else {
          fid_t fid = frag.GetFragId(u);
          std::tuple<std::pair<vid_t, vid_t>, bool, bool> msg =
              std::make_tuple(std::make_pair(u_gid, depth + 1), false, false);
          messages.SendToFragment(fid, msg);
          ret = true;
        }
      }
    }
    return ret;
  }

  void writeToCtx(const fragment_t& frag, context_t& ctx) {
    std::vector<typename fragment_t::oid_t> data;
    std::set<vid_t> pvisit;
    std::vector<vid_t> path;
    vid_t source_gid;
    frag.Oid2Gid(ctx.source_id, source_gid);
    pvisit.insert(source_gid);
    path.push_back(source_gid);
    int index = findVertexGlobalIndex(ctx, source_gid);
    generatePath(index, 0, path, pvisit, data, frag, ctx);
    std::vector<size_t> shape{static_cast<size_t>(ctx.path_num),
                              static_cast<size_t>(ctx.cutoff + 1)};
    if (ctx.path_num == 0) {
      data.push_back(1);
      shape.pop_back();
      shape[0] = 1;
    }
    VLOG(0) << "path_num: " << ctx.path_num << std::endl;
    ctx.assign(data, shape);
  }

  /**
   * @brief generate all the path from edge_map
   *
   * @param from
   * @param depth dfs depth
   * @param path vector contain current path
   * @param path set to record whether the vertex is accessed
   * @param data storage all paths
   * @param frag
   * @param ctx
   */
  void generatePath(int from, int depth, std::vector<vid_t>& path,
                    std::set<vid_t>& pvisit,
                    std::vector<typename fragment_t::oid_t>& data,
                    const fragment_t& frag, context_t& ctx) {
    // if depth == cutoff-1, just check set::targets.
    if (depth == (ctx.cutoff - 1)) {
      typename std::set<vid_t>::iterator it;
      for (it = ctx.targets.begin(); it != ctx.targets.end(); it++) {
        auto t = *it;
        if (pvisit.count(t) == 1) {
          continue;
        }
        int to = findVertexGlobalIndex(ctx, t);
        if (std::find(ctx.edge_map[from].begin(), ctx.edge_map[from].end(),
                      to) != ctx.edge_map[from].end()) {
          int len_counter = 0;
          for (auto gid : path) {
            data.push_back(frag.Gid2Oid(gid));
            len_counter++;
          }
          data.push_back(frag.Gid2Oid(t));
          len_counter++;
          // fill path to cutoff with -1
          while (len_counter != ctx.cutoff + 1) {
            data.push_back(-1);
            len_counter++;
          }
          ctx.path_num++;
        }
      }
      return;
    }

    for (uint64_t t = 0; t < ctx.edge_map[from].size(); t++) {
      int to = ctx.edge_map[from][t];
      vid_t gid = globalIndex2Gid(ctx, to);
      if (pvisit.count(gid) == 1) {
        continue;
      }
      pvisit.insert(gid);
      if (ctx.targets.count(gid)) {
        int len_counter = 0;
        for (auto v : path) {
          data.push_back(frag.Gid2Oid(v));
          len_counter++;
        }
        data.push_back(frag.Gid2Oid(gid));
        len_counter++;
        // fill path length to cutoff with -1
        while (len_counter != ctx.cutoff + 1) {
          data.push_back(-1);
          len_counter++;
        }
        ctx.path_num++;
      }
      path.push_back(gid);
      generatePath(to, depth + 1, path, pvisit, data, frag, ctx);
      path.pop_back();
      pvisit.erase(gid);
    }
  }

  /**
   * @brief gid to vertex global index
   *
   * @param ctx
   * @param gid
   */
  int findVertexGlobalIndex(context_t& ctx, vid_t gid) {
    int fid = static_cast<int>(gid >> ctx.fid_offset);
    int lid = static_cast<int>(gid & ctx.id_mask);
    int ret = 0;
    for (int i = 0; i < fid; i++) {
      ret += ctx.frag_vertex_num[i];
    }
    return ret + lid;
  }

  /**
   * @brief vertex global index to gid
   *
   * @param ctx
   * @param index
   */
  vid_t globalIndex2Gid(context_t& ctx, int index) {
    int i = 0;
    int sum = 0;
    int sum_last = 0;
    while (true) {
      sum += ctx.frag_vertex_num[i];
      if (sum > index) {
        int lid = index - sum_last;
        vid_t gid =
            (static_cast<vid_t>(i) << ctx.fid_offset) | static_cast<vid_t>(lid);
        return gid;
      }
      i++;
      sum_last = sum;
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_H_
