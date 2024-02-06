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
#ifndef ENGINES_HQPS_ENGINE_NULL_RECORD_H_
#define ENGINES_HQPS_ENGINE_NULL_RECORD_H_

#include <limits>
#include <tuple>
#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/engines/hqps_db/structures/path.h"

namespace gs {

struct None {};

static constexpr const None NONE;

template <typename T>
struct NullRecordCreator {
  static inline T GetNull() {
    static T null_value = std::numeric_limits<T>::max();
    return null_value;
  }
};

template <>
struct NullRecordCreator<std::string_view> {
  static inline std::string_view GetNull() { return ""; }
};

template <typename... T>
struct NullRecordCreator<std::tuple<T...>> {
  static inline std::tuple<T...> GetNull() {
    return std::make_tuple(NullRecordCreator<T>::GetNull()...);
  }
};

template <typename VID_T, typename LabelT>
struct NullRecordCreator<Path<VID_T, LabelT>> {
  static inline Path<VID_T, LabelT> GetNull() {
    return Path<VID_T, LabelT>::Null();
  }
};

template <size_t Ind = 0, typename... T>
static inline bool IsNull(const std::tuple<T...>& tuple) {
  if constexpr (Ind + 1 < sizeof...(T)) {
    return (std::get<0>(tuple) ==
            NullRecordCreator<
                std::tuple_element_t<0, std::tuple<T...>>>::GetNull()) &&
           IsNull<Ind + 1>(tuple);
  } else {
    return std::get<0>(tuple) ==
           NullRecordCreator<
               std::tuple_element_t<0, std::tuple<T...>>>::GetNull();
  }
}

template <typename T>
static inline bool IsNull(const std::vector<T>& vec) {
  if (vec.empty()) {
    return true;
  }
  for (auto& v : vec) {
    if (!IsNull(v)) {
      return false;
    }
  }
  return true;
}

template <typename T>
static inline bool IsNull(const T& opt) {
  return opt == NullRecordCreator<T>::GetNull();
}

template <typename VID_T>
static inline bool IsNull(const DefaultEdge<VID_T>& edge) {
  return IsNull(edge.src) || IsNull(edge.dst);
}

// customized operator ==
template <typename T>
bool operator==(const T& lhs, const None& rhs) {
  return IsNull(lhs);
}

template <typename T>
bool operator==(const None& lhs, const T& rhs) {
  return IsNull(rhs);
}

template <typename T>
bool operator!=(const T& lhs, const None& rhs) {
  return !IsNull(lhs);
}

template <typename T>
bool operator!=(const None& lhs, const T& rhs) {
  return !IsNull(rhs);
}
}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_NULL_RECORD_H_