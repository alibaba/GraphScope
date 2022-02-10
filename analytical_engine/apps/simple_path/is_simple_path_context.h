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

#ifndef ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_CONTEXT_H_

#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "grape/grape.h"

#include "apps/boundary/utils.h"
#include "core/context/tensor_context.h"
#include "core/object/dynamic.h"

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
   * @brief json format
   *  json = [(oid_t)node1,(oid_t)node2,....]
   *
   * @param messages
   * @param nodes_json
   */
  void Init(grape::DefaultMessageManager& messages,
            const std::string& nodes_json) {
    auto& frag = this->fragment();
    std::set<vid_t> visit;
    is_simple_path = true;
    vertex_t source;
    counter = 0;
    vid_t p1, p2;
    dynamic::Value path_nodes_array;
    dynamic::Parse(nodes_json, path_nodes_array);
    for (auto& node : path_nodes_array) {
      counter++;
      if (!frag.Oid2Gid(dynamic_to_oid<oid_t>(node), p1)) {
        LOG(ERROR) << "Input oid error" << std::endl;
        is_simple_path = false;
        break;
      }
      if (!visit.count(p1)) {
        visit.insert(p1);
      } else {
        is_simple_path = false;
        break;
      }
      if (counter != 1) {
        frag.Gid2Vertex(p2, source);
        if (frag.IsInnerVertex(source)) {
          pair_list.push_back(std::make_pair(p2, p1));
        }
      }
      p2 = p1;
    }
    if (counter == 0) {
      // empty list is not a valid path.
      is_simple_path = false;
    } else if (counter == 1) {
      // a list of one node is a path.
      if (frag.GetInnerVertex(dynamic_to_oid<oid_t>(path_nodes_array[0]),
                              source))
        is_simple_path = true;
      else
        is_simple_path = false;
    }
  }

  void Output(std::ostream& os) override {
    os << is_simple_path << std::endl;
    os << counter << std::endl;
  }

  std::vector<std::pair<vid_t, vid_t>> pair_list;
  int true_counter = 0;
  int counter;
  bool is_simple_path;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SIMPLE_PATH_IS_SIMPLE_PATH_CONTEXT_H_
