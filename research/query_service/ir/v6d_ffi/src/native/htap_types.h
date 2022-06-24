/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef SRC_CLIENT_DS_GRAPH_FRAGMENT_HTAP_TYPES_H_
#define SRC_CLIENT_DS_GRAPH_FRAGMENT_HTAP_TYPES_H_

#include <stdint.h>

#include <utility>

#include "vineyard/basic/ds/arrow_utils.h"

namespace vineyard {

namespace htap_types {

using OID_TYPE = int64_t;
using VID_TYPE = uint64_t;
using EID_TYPE = uint64_t;
using ORIGIN_EID_TYPE = int64_t;
using PROP_ID_TYPE = int;
using LABEL_ID_TYPE = int;

using ADJ_LIST_TYPE = std::pair<const EID_TYPE*, const EID_TYPE*>;
using VERTEX_RANGE_TYPE = std::pair<VID_TYPE, VID_TYPE>;

using VID_ARRAY = typename ConvertToArrowType<VID_TYPE>::ArrayType;
using VID_BUILDER = typename ConvertToArrowType<VID_TYPE>::BuilderType;

using OID_ARRAY = typename ConvertToArrowType<OID_TYPE>::ArrayType;
using OID_BUILDER = typename ConvertToArrowType<OID_TYPE>::BuilderType;

using EID_ARRAY = typename ConvertToArrowType<EID_TYPE>::ArrayType;
using EID_BUILDER = typename ConvertToArrowType<EID_TYPE>::BuilderType;

using ORIGIN_EID_ARRAY =
    typename ConvertToArrowType<ORIGIN_EID_TYPE>::ArrayType;
using ORIGIN_EID_BUILDER =
    typename ConvertToArrowType<ORIGIN_EID_TYPE>::BuilderType;

using LABEL_ARRAY = typename ConvertToArrowType<LABEL_ID_TYPE>::ArrayType;
using LABEL_BUILDER = typename ConvertToArrowType<LABEL_ID_TYPE>::BuilderType;

union PodProperties {
  bool bool_value;
  char char_value;
  int16_t int16_value;
  int int_value;
  int64_t long_value;
  float float_value;
  double double_value;
};

}  // namespace htap_types

}  // namespace vineyard

#endif  // SRC_CLIENT_DS_GRAPH_FRAGMENT_HTAP_TYPES_H_
