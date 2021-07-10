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

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_CONTEXT_H_

#include <limits>
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
class IsSimplePathContext : public TensorContext<FRAG_T, bool> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;

  explicit IsSimplePathContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, bool>(fragment) {}

  /**
   * @brief json formate
   *  josn = {"nodes" : [(oid_t)node1,(oid_t)node2,....]}
   *
   * @param messages
   * @param nodes_json
   */

  void Init(grape::DefaultMessageManager& messages,
            const std::string& nodes_json) {
    auto& frag = this->fragment();
    std::set<oid_t> visit;
    is_simple_path = true;
    vertex_t source;
    int counter = 0;
    oid_t pair_1 = 0;
    vid_t p1, p2;

    folly::dynamic nodes_array = folly::parseJson(nodes_json);
    for (auto val : nodes_array) {
      oid_t key = val.getInt();
      if (!visit.count(key)) {
        visit.insert(key);
      } else {
        is_simple_path = false;
        break;
      }
      counter++;
      if (counter == 1) {
      } else {
        if (frag.GetInnerVertex(pair_1, source)) {
          if (!frag.Oid2Gid(pair_1, p1) || !frag.Oid2Gid(key, p2)) {
            LOG(ERROR) << "Input oid error" << std::endl;
            break;
          }
          pair_list.push_back(std::make_pair(p1, p2));
        }
      }
      pair_1 = key;
    }
  }

  void Output(std::ostream& os) override { os << is_simple_path << std::endl; }

  std::vector<std::pair<vid_t, vid_t>> pair_list;
  int true_counter = 0;
  bool is_simple_path;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_CONTEXT_H_
