#ifndef RUNTIME_COMMON_UTILS_H_
#define RUNTIME_COMMON_UTILS_H_
#include <sstream>
#include <string>
#include <vector>

namespace gs {
namespace runtime {
struct ScanParams {
  int alias;
  std::vector<int> tables;

  ScanParams(int alias, const std::vector<int>& tables)
      : alias(alias), tables(tables) {}
  ScanParams() = default;

  std::string toString() const {
    std::stringstream ss;
    ss << "ScanParams(" << alias << ", ";
    ss << "{";
    for (size_t i = 0; i < tables.size(); ++i) {
      if (i + 1 == tables.size()) {
        ss << tables[i];
      } else {
        ss << tables[i] << ", ";
      }
    }
    ss << "})";
    return ss.str();
  }
};
}  // namespace runtime
}  // namespace gs

#endif
