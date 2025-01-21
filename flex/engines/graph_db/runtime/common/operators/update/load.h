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

#ifndef RUNTIME_COMMON_OPERATORS_UPDATE_LOAD_H_
#define RUNTIME_COMMON_OPERATORS_UPDATE_LOAD_H_

#include "flex/engines/graph_db/runtime/common/context.h"

namespace gs {

namespace runtime {

class Load {
 public:
  static WriteContext load_single_edge(
      GraphInsertInterface& graph, WriteContext&& ctxs, label_t src_label_id,
      label_t dst_label_id, label_t edge_label_id, PropertyType& src_pk_type,
      PropertyType& dst_pk_type, PropertyType& edge_prop_type, int src_index,
      int dst_index, int prop_index);

  static WriteContext load_single_vertex(
      GraphInsertInterface& graph, WriteContext&& ctxs, label_t label,
      PropertyType& pk_type, int id_col, const std::vector<int>& properties,
      const std::vector<std::tuple<label_t, label_t, label_t, PropertyType,
                                   PropertyType, PropertyType, int, int, int>>&
          edges);

  static WriteContext load(
      GraphInsertInterface& graph, WriteContext&& ctxs,
      const std::vector<std::tuple<label_t, int, PropertyType,
                                   std::vector<int>>>& vertex_mappings,
      const std::vector<std::tuple<label_t, label_t, label_t, PropertyType,
                                   PropertyType, PropertyType, int, int, int>>&
          edge_mappings);
};

}  // namespace runtime

}  // namespace gs

#endif  // RUNTIME_COMMON_OPERATORS_UPDATE_LOAD_H_