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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H_

#ifdef ENABLE_JAVA_SDK

#include "grape/utils/vertex_array.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"

#include "core/context/column.h"
#include "core/fragment/arrow_projected_fragment.h"

// Type alias for ease of use of some template types in Java.
namespace gs {

namespace arrow_projected_fragment_impl {

template <typename VID_T, typename EDATA_T>
using NbrDefault =
    Nbr<VID_T, vineyard::property_graph_types::EID_TYPE, EDATA_T>;

template <typename VID_T, typename EDATA_T>
using AdjListDefault =
    AdjList<VID_T, vineyard::property_graph_types::EID_TYPE, EDATA_T>;
}  // namespace arrow_projected_fragment_impl

// vineyard property graph utils
template <typename VID_T>
using NbrUnitDefault = vineyard::property_graph_utils::NbrUnit<
    VID_T, vineyard::property_graph_types::EID_TYPE>;

template <typename VID_T>
using NbrDefault = vineyard::property_graph_utils::Nbr<
    VID_T, vineyard::property_graph_types::VID_TYPE>;

template <typename VID_T>
using RawAdjListDefault = vineyard::property_graph_utils::RawAdjList<
    VID_T, vineyard::property_graph_types::EID_TYPE>;

template <typename VID_T>
using AdjListDefault = vineyard::property_graph_utils::AdjList<
    VID_T, vineyard::property_graph_types::EID_TYPE>;

template <typename DATA_T>
using EdgeDataColumnDefault = vineyard::property_graph_utils::EdgeDataColumn<
    DATA_T, NbrUnitDefault<vineyard::property_graph_types::VID_TYPE>>;

template <typename DATA_T>
using VertexDataColumnDefault =
    vineyard::property_graph_utils::VertexDataColumn<
        DATA_T, vineyard::property_graph_types::VID_TYPE>;

template <typename OID_T>
using ArrowFragmentDefault = vineyard::ArrowFragment<OID_T, uint64_t>;

template <typename DATA_T>
using VertexArrayDefault = grape::VertexArray<DATA_T, uint64_t>;

template <typename FRAG_T>
using DoubleColumn = Column<FRAG_T, double>;

template <typename FRAG_T>
using LongColumn = Column<FRAG_T, uint64_t>;

template <typename FRAG_T>
using IntColumn = Column<FRAG_T, uint32_t>;
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H_
