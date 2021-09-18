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
Author: Ma JingYuan
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
class AllSimplePathsContext : public TensorContext<FRAG_T, bool> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit AllSimplePathsContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, bool>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, oid_t source_id,
            const std::string& targets_json,
            int cutoff = std::numeric_limits<int>::max()) {
    auto& frag = this->fragment();
    this->source_id = source_id;
    this->id_mask = frag.id_mask();
    this->fid_offset = frag.fid_offset();

    if (cutoff == std::numeric_limits<int>::max()) {
      this->cutoff = frag.GetTotalVerticesNum() - 1;
    } else {
      this->cutoff = cutoff;
    }

    // init targets.
    std::set<vid_t> visit_p;
    vid_t v;
    std::vector<oid_t> oid_array;
    folly::dynamic nodes_array = folly::parseJson(targets_json);
    convert_to_oid_array(nodes_array, oid_array);
    for (const auto& val : oid_array) {
      if (!frag.Oid2Gid(val, v)) {
        LOG(ERROR) << "Graph not contain vertex " << val << std::endl;
        break;
      }
      if (!visit_p.count(v)) {
        visit_p.insert(v);
        targets.push_back(v);
      }
    }

    if (!frag.Oid2Gid(source_id, v)) {
      return;
    }
    if (visit_p.count(v) || this->cutoff < 1) {
      return;
    }

    vertex_t source;
    bool native_source = frag.GetInnerVertex(source_id, source);
    if (native_source) {
      int v_size = frag.GetTotalVerticesNum();
      VLOG(0) << "vsize: " << v_size << std::endl;
      frag_vertex_num.resize(frag.fnum());
      frag_vertex_num[frag.fid()] = frag.GetInnerVerticesNum();
    }
  }

  void Output(std::ostream& os) override {
    double counter_time = 0;

    auto& frag = this->fragment();
    vertex_t source;
    bool native_source = frag.GetInnerVertex(source_id, source);
    std::vector<vid_t> q;
    if (native_source) {
      std::set<vid_t> qvisit;
      vid_t source_gid = frag.Vertex2Gid(source);
      qvisit.insert(source_gid);
      q.push_back(source_gid);
      int index = find_edge_map_index(source_gid);

      struct timeval t;
      gettimeofday(&t, 0);
      counter_time -= static_cast<double>(t.tv_sec) +
                      static_cast<double>(t.tv_usec) / 1000000;
      Pint_Result(index, 0, q, qvisit, os);
      gettimeofday(&t, 0);
      counter_time += static_cast<double>(t.tv_sec) +
                      static_cast<double>(t.tv_usec) / 1000000;
      VLOG(0) << "counter_time : " << counter_time << std::endl;
    }
    VLOG(0) << "exec_time : " << exec_time << std::endl;
  }

  void Pint_Result(int from, int depth, std::vector<vid_t>& q,
                   std::set<vid_t>& qvisit, std::ostream& os) {
    auto& frag = this->fragment();
    if (depth == (this->cutoff - 1)) {
      // VLOG(0) << "in last: " << std::endl;
      for (auto t : targets) {
        if (qvisit.count(t) == 1) {
          continue;
        }
        int to = find_edge_map_index(t);
        if (std::find(edge_map[from].begin(), edge_map[from].end(), to) !=
            edge_map[from].end()) {
          for (auto gid : q) {
            os << frag.Gid2Oid(gid) << " ";
          }
          os << frag.Gid2Oid(t) << " " << std::endl;
        }
      }
      return;
    }

    for (long unsigned int t = 0; t < edge_map[from].size(); t++) {
      int to = edge_map[from][t];
      vid_t gid = index2gid(to);
      if (qvisit.count(gid) == 1) {
        continue;
      }
      qvisit.insert(gid);
      if (std::find(targets.begin(), targets.end(), gid) != targets.end()) {
        for (auto v : q) {
          os << frag.Gid2Oid(v) << " ";
        }
        os << frag.Gid2Oid(gid) << " " << std::endl;
      }
      q.push_back(gid);
      Pint_Result(to, depth + 1, q, qvisit, os);
      q.pop_back();
      qvisit.erase(gid);
    }
  }

  int find_edge_map_index(vid_t gid) {
    int fid = (int) (gid >> fid_offset);
    int lid = (int) (gid & id_mask);
    int ret = 0;
    for (int i = 0; i < fid; i++) {
      ret += frag_vertex_num[i];
    }
    return ret + lid;
  }

  vid_t index2gid(int index) {
    int i = 0;
    int sum = 0;
    int sum_last = 0;
    while (true) {
      sum += frag_vertex_num[i];
      if (sum > index) {
        int lid = index - sum_last;
        vid_t gid = (vid_t) ((i << fid_offset) | lid);
        return gid;
      }
      i++;
      sum_last = sum;
    }
  }

  oid_t source_id;
  std::queue<std::pair<vid_t, int>> curr_level_inner, next_level_inner;
  std::set<vid_t> visit;
  std::vector<vid_t> targets;
  std::vector<vid_t> frag_vertex_num;
  int cutoff;
  bool source_flag = false;
  fid_t soucre_fid;
  vid_t id_mask;
  int fid_offset;
  std::vector<std::vector<int>> edge_map;

  double exec_time = 0;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_CONTEXT_H_
