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

#ifndef ANALYTICAL_ENGINE_CORE_UTILS_TRAIT_UTILS_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_TRAIT_UTILS_H_

#include <type_traits>

#include "vineyard/common/util/static_if.h"

namespace gs {
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename VERTEX_MAP_T>
class ArrowFlattenedFragment;

template <typename T>
struct is_flattened_fragment {
  using type = std::false_type;
  static constexpr bool value = false;
};

template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T,
          typename VERTEX_MAP_T>
struct is_flattened_fragment<
    ArrowFlattenedFragment<OID_T, VID_T, VDATA_T, EDATA_T, VERTEX_MAP_T>> {
  using type = std::true_type;
  static constexpr bool value = true;
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_UTILS_TRAIT_UTILS_H_
