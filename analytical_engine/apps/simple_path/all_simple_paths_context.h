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

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_CONTEXT_H_

#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "folly/dynamic.h"
#include "folly/json.h"
#include "grape/grape.h"

#include "apps/boundary/utils.h"
#include "core/context/tensor_context.h"

namespace gs {

template <typename FRAG_T>
class AllSimplePathsContext
    : public TensorContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit AllSimplePathsContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, typename FRAG_T::oid_t>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, oid_t source_id,
            const std::string& targets_json,
            int cutoff = std::numeric_limits<int>::max()) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    this->source_id = source_id;
    this->id_mask = frag.id_mask();
    this->fid_offset = frag.fid_offset();

    if (cutoff == std::numeric_limits<int>::max()) {
      this->cutoff = frag.GetTotalVerticesNum() - 1;
    } else {
      this->cutoff = cutoff;
    }

    // init targets.
    vid_t gid;
    folly::dynamic target_nodes_array = folly::parseJson(targets_json);
    for (const auto& node : target_nodes_array) {
      frag.Oid2Gid(dynamic_to_oid<oid_t>(node), gid);
      targets.insert(gid);
    }

    visited.Init(vertices, false);
    vertex_t source;
    native_source = frag.GetInnerVertex(source_id, source);
    if (native_source) {
      frag_vertex_num.resize(frag.fnum());
      frag_vertex_num[frag.fid()] = frag.GetInnerVerticesNum();
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    vertex_t source;
    std::vector<vid_t> path;
    if (native_source) {
      std::vector<bool> vertex_visited;
      vid_t source_gid = frag.Vertex2Gid(source);
      path.push_back(source_gid);
      vid_t source_index = Gid2GlobalIndex(source_gid);
      vertex_visited[source_index] = true;
      printResult(source_index, 0, path, vertex_visited, os);
    }
  }

  /**
   * @brief gid to vertex global index
   *
   * @param gid
   */
  vid_t Gid2GlobalIndex(vid_t gid) {
    vid_t fid = gid >> fid_offset;
    vid_t lid = gid & id_mask;
    if (fid == 0)
      return lid;
    else
      return frag_vertex_num[fid - 1] + lid;
  }

  /**
   * @brief vertex global index to gid
   *
   * @param index
   */
  vid_t GlobalIndex2Gid(vid_t index) {
    vid_t i = 0;
    vid_t lid;
    while (true) {
      if (index < frag_vertex_num[i])
        break;
      i++;
    }
    if (i == 0)
      lid = index;
    else
      lid = index - frag_vertex_num[i - 1];
    return (i << fid_offset) | lid;
  }

  oid_t source_id;
  std::queue<std::pair<vid_t, int>> curr_level_inner, next_level_inner;
  typename FRAG_T::template vertex_array_t<bool> visited;
  std::set<vid_t> targets;
  std::vector<vid_t> frag_vertex_num;
  int cutoff;
  bool native_source = false;
  fid_t soucre_fid;
  vid_t id_mask;
  int fid_offset;
  std::vector<std::vector<vid_t>> simple_paths_edge_map;
  int frag_finish_counter = 0;
  int path_num = 0;

 private:
  void printResult(int from, int depth, std::vector<vid_t>& path,
                   std::vector<bool>& vertex_visited, std::ostream& os) {
    auto& frag = this->fragment();
    if (depth == (this->cutoff - 1)) {
      typename std::set<vid_t>::iterator it;
      for (it = targets.begin(); it != targets.end(); it++) {
        auto t = *it;
        vid_t to = Gid2GlobalIndex(t);
        if (vertex_visited[to]) {
          continue;
        }
        if (std::find(simple_paths_edge_map[from].begin(),
                      simple_paths_edge_map[from].end(),
                      to) != simple_paths_edge_map[from].end()) {
          for (auto gid : path) {
            os << frag.Gid2Oid(gid) << " ";
          }
          os << frag.Gid2Oid(t) << " " << std::endl;
        }
      }
      return;
    }

    for (uint64_t t = 0; t < simple_paths_edge_map[from].size(); t++) {
      vid_t to = simple_paths_edge_map[from][t];
      vid_t gid = GlobalIndex2Gid(to);
      if (vertex_visited[to]) {
        continue;
      }
      vertex_visited[to] = true;
      if (targets.count(gid)) {
        for (auto v : path) {
          os << frag.Gid2Oid(v) << " ";
        }
        os << frag.Gid2Oid(gid) << " " << std::endl;
      }
      path.push_back(gid);
      printResult(to, depth + 1, path, vertex_visited, os);
      path.pop_back();
      vertex_visited[to] = false;
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_CONTEXT_H_
