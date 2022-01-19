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

#ifndef ANALYTICAL_ENGINE_CORE_APP_PREGEL_EXPORT_H_
#define ANALYTICAL_ENGINE_CORE_APP_PREGEL_EXPORT_H_

#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/app/pregel/aggregators/aggregator.h"
#include "core/app/pregel/i_vertex_program.h"
#include "core/app/pregel/pregel_property_vertex.h"

namespace pregel {

#if defined(_OID_TYPE)
template <typename VD_T, typename MD_T>
using Vertex = gs::PregelPropertyVertex<
    vineyard::ArrowFragment<_OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;

template <typename VD_T, typename MD_T>
using Context = gs::PregelPropertyComputeContext<
    vineyard::ArrowFragment<_OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;

template <typename VD_T, typename MD_T>
using Neighbor = gs::PregelPropertyNeighbor<
    vineyard::ArrowFragment<_OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;

template <typename VD_T, typename MD_T>
using AdjList = gs::PregelPropertyAdjList<
    vineyard::ArrowFragment<_OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;
#else
template <typename VD_T, typename MD_T>
using Vertex = gs::PregelPropertyVertex<
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;

template <typename VD_T, typename MD_T>
using Context = gs::PregelPropertyComputeContext<
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;

template <typename VD_T, typename MD_T>
using Neighbor = gs::PregelPropertyNeighbor<
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;

template <typename VD_T, typename MD_T>
using AdjList = gs::PregelPropertyAdjList<
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;
#endif

using gs::Aggregator;
using gs::MessageIterator;
using gs::PregelAggregatorType;

}  // namespace pregel

#endif  // ANALYTICAL_ENGINE_CORE_APP_PREGEL_EXPORT_H_
