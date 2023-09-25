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
#ifndef ENGINES_HQPS_APP_CYPHER_APP_BASE_H_
#define ENGINES_HQPS_APP_CYPHER_APP_BASE_H_

#include "flex/engines/hqps_db/database/mutable_csr_interface.h"
#include "flex/proto_generated_gie/results.pb.h"
#include "flex/utils/app_utils.h"

namespace gs {

enum class GraphStoreType {
  Grape = 0,
};

template <typename GRAPH_TYPE>
class HqpsAppBase {
 public:
  /**
   * @brief Construct a new Hqps App Base object
   */
  virtual ~HqpsAppBase() = default;
  /**
   * @brief Query the graph with the given input
   *
   * @param graph The graph to query
   * @param input The input to query
   * @return virtual results::CollectiveResults The query result
   */
  virtual results::CollectiveResults Query(const GRAPH_TYPE& graph,
                                           Decoder& input) const = 0;
};

}  // namespace gs

#endif  // ENGINES_HQPS_APP_CYPHER_APP_BASE_H_