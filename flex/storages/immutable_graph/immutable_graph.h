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

#ifndef STORAGES_IMMUTABLE_GRAPH_IMMUTABLE_GRAPH_H_
#define STORAGES_IMMUTABLE_GRAPH_IMMUTABLE_GRAPH_H_

#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/types.h"

namespace immutable_graph {

template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          grape::LoadStrategy load_strategy, typename VM_T>
using ImmutableGraph =
    grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                    load_strategy, VM_T>;

}  // namespace immutable_graph

#endif  // STORAGES_IMMUTABLE_GRAPH_IMMUTABLE_GRAPH_H_
