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

#include <string>

#include "grape/utils/vertex_array.h"
#include "vineyard/common/util/arrow.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"

#include "arrow/array.h"

#include "core/context/column.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/fragment/arrow_projected_fragment_mapper.h"
#include "core/java/fragment_getter.h"

// Type alias for ease of use of some template types in Java.
namespace gs {

namespace arrow_projected_fragment_impl {

template <typename VID_T, typename EDATA_T>
using NbrDefault =
    Nbr<VID_T, vineyard::property_graph_types::EID_TYPE, EDATA_T>;

template <typename VID_T>
using NbrStrData =
    Nbr<VID_T, vineyard::property_graph_types::EID_TYPE, std::string>;

template <typename VID_T, typename EDATA_T>
using AdjListDefault =
    AdjList<VID_T, vineyard::property_graph_types::EID_TYPE, EDATA_T>;

template <typename VID_T>
using AdjListStrData =
    AdjList<VID_T, vineyard::property_graph_types::EID_TYPE, std::string>;
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

template <typename OID_T, typename VID_T, typename VDATA_T>
using ArrowProjectedStringEDFragment =
    ArrowProjectedFragment<OID_T, VID_T, VDATA_T, std::string>;

template <typename OID_T, typename VID_T, typename EDATA_T>
using ArrowProjectedStringVDFragment =
    ArrowProjectedFragment<OID_T, VID_T, std::string, EDATA_T>;

template <typename OID_T, typename VID_T>
using ArrowProjectedStringVEDFragment =
    ArrowProjectedFragment<OID_T, VID_T, std::string, std::string>;

// mapper
template <typename OID_T, typename VID_T, typename VDATA_T>
using ArrowProjectedStringEDFragmentMapper =
    ArrowProjectedFragmentMapper<OID_T, VID_T, VDATA_T, std::string>;

template <typename OID_T, typename VID_T, typename EDATA_T>
using ArrowProjectedStringVDFragmentMapper =
    ArrowProjectedFragmentMapper<OID_T, VID_T, std::string, EDATA_T>;

template <typename OID_T, typename VID_T>
using ArrowProjectedStringVEDFragmentMapper =
    ArrowProjectedFragmentMapper<OID_T, VID_T, std::string, std::string>;

// Getter
template <typename OID_T, typename VID_T, typename VDATA_T>
using ArrowProjectedStringEDFragmentGetter =
    ArrowProjectedFragmentGetter<OID_T, VID_T, VDATA_T, std::string>;

template <typename OID_T, typename VID_T, typename EDATA_T>
using ArrowProjectedStringVDFragmentGetter =
    ArrowProjectedFragmentGetter<OID_T, VID_T, std::string, EDATA_T>;

template <typename OID_T, typename VID_T>
using ArrowProjectedStringVEDFragmentGetter =
    ArrowProjectedFragmentGetter<OID_T, VID_T, std::string, std::string>;

template <typename DATA_T>
using VertexArrayDefault =
    grape::VertexArray<grape::VertexRange<uint64_t>, DATA_T>;

template <typename VID_T, typename DATA_T>
using JavaVertexArray = grape::VertexArray<grape::VertexRange<VID_T>, DATA_T>;

template <typename FRAG_T>
using DoubleColumn = Column<FRAG_T, double>;

template <typename FRAG_T>
using LongColumn = Column<FRAG_T, uint64_t>;

template <typename FRAG_T>
using IntColumn = Column<FRAG_T, uint32_t>;

template <typename T>
struct ConvertToArrowType {
  using BuilderType = typename vineyard::ConvertToArrowType<T>::BuilderType;
  using ArrayType = typename vineyard::ConvertToArrowType<T>::ArrayType;
};

/** Add one specialization for string_view, we will need this in Java FFI.*/
template <>
struct ConvertToArrowType<vineyard::arrow_string_view> {
  using BuilderType =
      typename vineyard::ConvertToArrowType<std::string>::BuilderType;
  using ArrayType =
      typename vineyard::ConvertToArrowType<std::string>::ArrayType;
};

template <typename T>
using ArrowArrayBuilder = typename ConvertToArrowType<T>::BuilderType;

template <typename T>
using ArrowArray = typename ConvertToArrowType<T>::ArrayType;

}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H_
