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

#include "apps/simple_path/utils.h"
#include "folly/dynamic.h"
#include "folly/json.h"
#include "grape/grape.h"

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
    std::vector<oid_t> target_oid_array;
    folly::dynamic target_nodes_id_array = folly::parseJson(targets_json);
    ExtractOidArrayFromDynamic(target_nodes_id_array, target_oid_array);
    for (const auto& oid : target_oid_array) {
      if (!frag.Oid2Gid(oid, gid)) {
        LOG(ERROR) << "Graph not contain vertex " << oid << std::endl;
        nodes_not_found = true;
        break;
      }
      if (!targets.count(gid)) {
        targets.insert(gid);
      }
    }

    if (!frag.Oid2Gid(source_id, gid) || targets.count(gid) ||
        this->cutoff < 1) {
      return;
    }

    visited.Init(vertices, false);
    vertex_t source;
    bool native_source = frag.GetInnerVertex(source_id, source);
    if (native_source) {
      frag_vertex_num.resize(frag.fnum());
      frag_vertex_num[frag.fid()] = frag.GetInnerVerticesNum();
    }
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    vertex_t source;
    bool native_source = frag.GetInnerVertex(source_id, source);
    std::vector<vid_t> path;
    if (native_source) {
      std::set<vid_t> vertex_visit;
      vid_t source_gid = frag.Vertex2Gid(source);
      vertex_visit.insert(source_gid);
      path.push_back(source_gid);
      vid_t index = Gid2VertexGlobalIndex(source_gid);
      printResult(index, 0, path, vertex_visit, os);
    }
  }

  vid_t Gid2VertexGlobalIndex(vid_t gid) {
    vid_t fid = gid >> fid_offset;
    vid_t lid = gid & id_mask;
    vid_t ret = 0;
    for (vid_t i = 0; i < fid; i++) {
      ret += frag_vertex_num[i];
    }
    return ret + lid;
  }

  vid_t GlobalIndex2Gid(vid_t index) {
    vid_t i = 0;
    vid_t sum = 0;
    vid_t sum_last = 0;
    while (true) {
      sum += frag_vertex_num[i];
      if (sum > index) {
        vid_t lid = index - sum_last;
        vid_t gid = (i << fid_offset) | lid;
        return gid;
      }
      i++;
      sum_last = sum;
    }
  }

  oid_t source_id;
  std::queue<std::pair<vid_t, int>> curr_level_inner, next_level_inner;
  typename FRAG_T::template vertex_array_t<bool> visited;
  std::set<vid_t> visit;
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
  bool nodes_not_found = false;

 private:
  void printResult(int from, int depth, std::vector<vid_t>& path,
                   std::set<vid_t>& vertex_visit, std::ostream& os) {
    auto& frag = this->fragment();
    if (depth == (this->cutoff - 1)) {
      typename std::set<vid_t>::iterator it;
      for (it = targets.begin(); it != targets.end(); it++) {
        auto t = *it;
        if (vertex_visit.count(t) == 1) {
          continue;
        }
        vid_t to = Gid2VertexGlobalIndex(t);
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
      if (vertex_visit.count(gid) == 1) {
        continue;
      }
      vertex_visit.insert(gid);
      if (targets.count(gid)) {
        for (auto v : path) {
          os << frag.Gid2Oid(v) << " ";
        }
        os << frag.Gid2Oid(gid) << " " << std::endl;
      }
      path.push_back(gid);
      printResult(to, depth + 1, path, vertex_visit, os);
      path.pop_back();
      vertex_visit.erase(gid);
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_CONTEXT_H_
