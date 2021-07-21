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

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_CONTEXT_H_

#include <limits>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

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

  explicit AllSimplePathsContext(const FRAG_T &fragment)
      : TensorContext<FRAG_T, bool>(fragment) {}

  void Init(grape::DefaultMessageManager &messages, oid_t source_id,
            const std::string &targets_json, int cutoff = INT_MAX) {
    std::set<vid_t> visit;
    vid_t p;
    auto &frag = this->fragment();
    this->source_id = source_id;

    if (cutoff == INT_MAX) {
      this->cutoff = frag.GetTotalVerticesNum() - 1;
    } else {
      this->cutoff = cutoff;
    }
    VLOG(0) << "frag id: " << frag.fid() << " vertex num: " << this->cutoff
            << std::endl;

    // init targets.
    folly::dynamic nodes_array = folly::parseJson(targets_json);
    for (const auto &val : nodes_array) {
      oid_t key = val.getInt(); // pass cmake
      if (!frag.Oid2Gid(key, p)) {
        LOG(ERROR) << "Input oid error" << std::endl;
        break;
      }
      if (!visit.count(p)) {
        visit.insert(p);
        targets.push_back(p);
      }
    }
  }

  void Output(std::ostream &os) override {
    auto &frag = this->fragment();
    // os<<"num of result"<< result_queue.size()<<std::endl;
    while (!result_queue.empty()) {
      std::vector<vid_t> s = result_queue.front();
      result_queue.pop();
      // os<<"size"<<s.size()<<std::endl;
      for (auto e : s) {
        os << frag.Gid2Oid(e) << " ";
      }
      os << std::endl;
    }
  }

  oid_t source_id;
  std::queue<std::vector<vid_t>> curr_level_inner, next_level_inner;
  std::vector<vid_t> targets;
  int cutoff;
  int depth;
  std::queue<std::vector<vid_t>> result_queue;
};
} // namespace gs

#endif // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_ALL_SIMPLE_PATHS_CONTEXT_H_
