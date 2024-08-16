#ifndef RUNTIME_CODEGEN_UTILS_UTILS_H_
#define RUNTIME_CODEGEN_UTILS_UTILS_H_
#include <string>
#include <vector>

namespace gs {
namespace runtime {
template <typename T>
std::string vec_2_str(const std::vector<T>& vec) {
  if constexpr (std::is_same_v<T, std::string>) {
    std::string ret = "{";
    for (size_t i = 0; i < vec.size(); ++i) {
      ret += "\"" + vec[i] + "\"";
      if (i + 1 != vec.size()) {
        ret += ", ";
      }
    }
    ret += "}";
    return ret;
  } else {
    std::string ret = "{";
    for (size_t i = 0; i < vec.size(); ++i) {
      ret += std::to_string(vec[i]);
      if (i + 1 != vec.size()) {
        ret += ", ";
      }
    }
    ret += "}";
    return ret;
  }
}
}  // namespace runtime
}  // namespace gs
#endif