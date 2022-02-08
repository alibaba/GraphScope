/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
      grape::MessageStrategy::kSyncOnOuterVertex;
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;
  enum MsgType { init_msg, bfs_msg, edge_map_msg };
  using oid_t = typename fragment_t::oid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using msg_t = typename std::tuple<enum MsgType, vid_t, vid_t>;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    vertex_t source;
    vid_t source_gid;

    frag.Oid2Gid(ctx.source_id, source_gid);
    ctx.source_fid = source_gid >> ctx.fid_offset;
    // If is native_source, resize the edge_map, otherwise send the fragment
    // inner vertex num to source-vertex's fragment.
    if (ctx.native_source) {
      ctx.simple_paths_edge_map.resize(frag.GetTotalVerticesNum());
    }
    vid_t inner_num = frag.GetInnerVerticesNum();
    fid_t fid = frag.fid();
    MsgType msg_type = init_msg;
    msg_t msg = std::make_tuple(msg_type, (vid_t) fid, inner_num);
    messages.SendToFragment(ctx.source_fid, msg);
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    int frag_finish_counter = 0;
    int init_counter = 0;
    msg_t msg;

    while (messages.GetMessage(msg)) {
      // msg_type = init_msg means the message is to init frag_vertex_num,
      // msg[1] is frag's gid and msg[2] is vertex's depth.
      // msg_type = bfs_msg means the message is the
      // vertices waiting to be accessed, msg[1] is frag's gid and
      // msg[2] is vertex's depth.
      // msg_type = edge_map_msg means the message is send to source frag to
      // update edge_map. The msg[1] is from's gid and msg[2] is to's gid.
      MsgType msg_type = std::get<0>(msg);
      if (msg_type == init_msg) {
        vid_t fid = std::get<1>(msg);
        vid_t inner_num = std::get<2>(msg);
        init_counter++;
        ctx.frag_vertex_num[fid] = inner_num;
        if (init_counter == static_cast<int>(frag.fnum())) {
          reloadFragVertexNum(ctx.frag_vertex_num);
          vertex_t source;
          frag.GetInnerVertex(ctx.source_id, source);
          bfs(source, frag, ctx, messages, 0);
          ctx.visited[source] = true;
          messages.ForceContinue();
          frag_finish_counter++;
        }
      } else if (msg_type == bfs_msg) {
        vid_t gid = std::get<1>(msg);
        vid_t depth = std::get<2>(msg);
        vertex_t v;
        frag.InnerVertexGid2Vertex(gid, v);
        if (!ctx.visited[v]) {
          ctx.visited[v] = true;
          ctx.next_level_inner.push(std::make_pair(gid, depth));
        }
      } else if (msg_type == edge_map_msg) {
        vid_t from_gid = std::get<1>(msg);
        vid_t to_gid = std::get<2>(msg);
        vid_t u_index = ctx.Gid2GlobalIndex(from_gid);
        vid_t v_index = ctx.Gid2GlobalIndex(to_gid);
        ctx.simple_paths_edge_map[u_index].push_back(v_index);
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
        if (bfs(v, frag, ctx, messages, depth))
          frag_finish_counter++;
      }
    }
    Sum(frag_finish_counter, ctx.frag_finish_counter);
    if (!ctx.next_level_inner.empty() || ctx.frag_finish_counter > 0) {
      messages.ForceContinue();
    } else {
      if (ctx.frag_finish_counter == 0 && frag.fid() == ctx.source_fid) {
        writeToCtx(frag, ctx);
      }
    }
  }

 private:
  bool bfs(vertex_t v, const fragment_t& frag, context_t& ctx,
           message_manager_t& messages, int depth) {
    bool ret = false;
    std::set<vid_t> vertex_visit;
    vid_t gid = frag.Vertex2Gid(v);
    auto oes = frag.GetOutgoingAdjList(v);
    for (auto& e : oes) {
      vertex_t u = e.get_neighbor();
      vid_t u_gid = frag.Vertex2Gid(u);
      if (vertex_visit.count(u_gid) == 0) {
        vertex_visit.insert(u_gid);
        if (ctx.native_source == false) {
          MsgType msg_type = edge_map_msg;
          msg_t msg = std::make_tuple(msg_type, gid, u_gid);
          messages.SendToFragment(ctx.source_fid, msg);
          ret = true;
        } else {
          vid_t u_index = ctx.Gid2GlobalIndex(gid);
          vid_t v_index = ctx.Gid2GlobalIndex(u_gid);
          ctx.simple_paths_edge_map[u_index].push_back(v_index);
        }
        if (frag.IsInnerVertex(u)) {
          if (!ctx.visited[u]) {
            ctx.visited[u] = true;
            ctx.next_level_inner.push(std::make_pair(u_gid, depth + 1));
          }
        } else {
          fid_t fid = frag.GetFragId(u);
          MsgType msg_type = bfs_msg;
          msg_t msg = std::make_tuple(msg_type, u_gid, depth + 1);
          messages.SendToFragment(fid, msg);
          ret = true;
        }
      }
    }
    return ret;
  }

  void writeToCtx(const fragment_t& frag, context_t& ctx) {
    std::vector<typename fragment_t::oid_t> data;
    std::vector<bool> vertex_visited;
    std::vector<vid_t> path;
    vid_t source_gid;
    vertex_visited.resize(frag.GetTotalVerticesNum());
    frag.Oid2Gid(ctx.source_id, source_gid);
    vid_t source_index = ctx.Gid2GlobalIndex(source_gid);
    vertex_visited[source_index] = true;
    path.push_back(source_gid);
    generatePath(source_index, 0, path, vertex_visited, data, frag, ctx);
    std::vector<size_t> shape{static_cast<size_t>(ctx.path_num),
                              static_cast<size_t>(ctx.cutoff + 1)};
    if (ctx.path_num == 0) {
      data.push_back(oid_t(1));
      shape.pop_back();
      shape[0] = 1;
    }
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
                    std::vector<bool>& vertex_visited,
                    std::vector<typename fragment_t::oid_t>& data,
                    const fragment_t& frag, context_t& ctx) {
    // if depth == cutoff-1, just check set::targets.
    if (depth == (ctx.cutoff - 1)) {
      typename std::set<vid_t>::iterator it;
      for (it = ctx.targets.begin(); it != ctx.targets.end(); it++) {
        auto t = *it;
        vid_t to = ctx.Gid2GlobalIndex(t);
        if (vertex_visited[to]) {
          continue;
        }
        if (std::find(ctx.simple_paths_edge_map[from].begin(),
                      ctx.simple_paths_edge_map[from].end(),
                      to) != ctx.simple_paths_edge_map[from].end()) {
          int len_counter = 0;
          for (auto gid : path) {
            data.push_back(frag.Gid2Oid(gid));
            len_counter++;
          }
          data.push_back(frag.Gid2Oid(t));
          len_counter++;
          // fill path to cutoff with -1
          while (len_counter != ctx.cutoff + 1) {
            data.push_back(oid_t(-1));
            len_counter++;
          }
          ctx.path_num++;
        }
      }
      return;
    }

    for (uint64_t t = 0; t < ctx.simple_paths_edge_map[from].size(); t++) {
      vid_t to = ctx.simple_paths_edge_map[from][t];
      vid_t gid = ctx.GlobalIndex2Gid(to);
      if (vertex_visited[to]) {
        continue;
      }
      vertex_visited[to] = true;
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
          data.push_back(oid_t(-1));
          len_counter++;
        }
        ctx.path_num++;
      }
      path.push_back(gid);
      generatePath(to, depth + 1, path, vertex_visited, data, frag, ctx);
      path.pop_back();
      vertex_visited[to] = false;
    }
  }

  void reloadFragVertexNum(std::vector<vid_t>& frag_vertex_num) {
    vid_t sum = 0;
    for (vid_t i = 0; i < frag_vertex_num.size(); i++) {
      sum += frag_vertex_num[i];
      frag_vertex_num[i] = sum;
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_H_
