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

namespace gs {

struct None {};

static constexpr const None NONE;

template <typename T>
struct NullRecordCreator {
  static inline T GetNull() {
    if constexpr (std::numeric_limits<std::remove_const_t<
                      std::remove_reference_t<T>>>::is_specialized) {
      using type = std::remove_const_t<std::remove_reference_t<T>>;
      static type null_value = std::numeric_limits<type>::max();
      return null_value;
    } else {
      return T::GetNull();
    }
  }
};

template <>
struct NullRecordCreator<std::string_view> {
  static inline std::string_view GetNull() { return ""; }
};

template <>
struct NullRecordCreator<Direction> {
  static inline Direction GetNull() { return Direction::Unknown; }
};

template <>
struct NullRecordCreator<Date> {
  static inline Date GetNull() {
    return Date(std::numeric_limits<int64_t>::max());
  }
};

template <>
struct NullRecordCreator<Any> {
  static inline Any GetNull() { return Any(); }
};

template <>
struct NullRecordCreator<LabelKey> {
  static inline LabelKey GetNull() {
    return LabelKey(std::numeric_limits<LabelKey::label_data_type>::max());
  }
};

template <>
struct NullRecordCreator<grape::EmptyType> {
  static inline grape::EmptyType GetNull() { return grape::EmptyType(); }
};

// bool doesn't have a null value

template <>
struct NullRecordCreator<GlobalId> {
  static inline GlobalId GetNull() {
    return GlobalId(std::numeric_limits<GlobalId::gid_t>::max());
  }
};

template <typename... T>
struct NullRecordCreator<std::tuple<T...>> {
  static inline std::tuple<T...> GetNull() {
    return std::make_tuple(NullRecordCreator<T>::GetNull()...);
  }
};

static inline bool IsNull(const bool& b) { return false; }

template <typename A, typename B>
static inline bool IsNull(const std::pair<A, B>& pair) {
  return IsNull(pair.first) && IsNull(pair.second);
}

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

template <size_t Is, typename T>
bool has_null(const T& t) {
  if constexpr (Is < std::tuple_size<T>::value) {
    if (IsNull(std::get<Is>(t))) {
      return true;
    } else {
      return has_null<Is + 1>(t);
    }
  } else {
    return false;
  }
}

template <typename... T>
bool HasNull(const std::tuple<T...>& t) {
  // at least one element in the tuple is null
  return has_null<0>(t);
}

template <typename T>
bool HasNull(const T& t) {
  return IsNull(t);
}

}  // namespace gs

#endif  // ENGINES_HQPS_ENGINE_NULL_RECORD_H_