#ifndef NULL_RECORD_H
#define NULL_RECORD_H

#include <limits>
#include <tuple>
#include "flex/engines/hqps/engine/hqps_utils.h"

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

// customized operator ==
template <typename T>
bool operator==(const T& lhs, const None& rhs) {
  return IsNull(lhs);
}
}  // namespace gs

#endif  // NULL_RECORD_H