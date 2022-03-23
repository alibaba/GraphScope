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

#ifndef ANALYTICAL_ENGINE_APPS_PYTHON_PIE_EXPORT_H_
#define ANALYTICAL_ENGINE_APPS_PYTHON_PIE_EXPORT_H_

#include "grape/grape.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "apps/python_pie/wrapper.h"

namespace python_grape {

using Fragment = gs::PythonPIEFragment<vineyard::ArrowFragment<
    _OID_TYPE, vineyard::property_graph_types::VID_TYPE>>;

template <typename VD_T, typename MD_T>
using Context = gs::PythonPIEComputeContext<
    vineyard::ArrowFragment<_OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>,
    VD_T, MD_T>;

using vid_t = typename vineyard::ArrowFragment<
    _OID_TYPE, vineyard::property_graph_types::VID_TYPE>::vid_t;
using Vertex = typename vineyard::ArrowFragment<
    _OID_TYPE, vineyard::property_graph_types::VID_TYPE>::vertex_t;
using Nbr = typename vineyard::ArrowFragment<
    _OID_TYPE, vineyard::property_graph_types::VID_TYPE>::nbr_t;
using AdjList = typename gs::PIEAdjList<vineyard::ArrowFragment<
    _OID_TYPE, vineyard::property_graph_types::VID_TYPE>>;
using VertexRange = typename vineyard::ArrowFragment<
    _OID_TYPE, vineyard::property_graph_types::VID_TYPE>::vertex_range_t;
using VertexIterator = typename VertexRange::iterator;

using grape::MessageStrategy;
using gs::PIEAggregateType;

template <typename T>
using VertexArray = grape::VertexArray<VertexRange, T>;
}  // namespace python_grape

#endif  // ANALYTICAL_ENGINE_APPS_PYTHON_PIE_EXPORT_H_
