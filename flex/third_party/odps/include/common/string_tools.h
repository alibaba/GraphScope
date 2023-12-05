#pragma once

#include <ctype.h>
#include <string>

namespace apsara {
namespace odps {
namespace sdk {
namespace storage_api {

inline std::string ToLowerCaseString(const std::string& orig) {
  std::string lowerCase(orig);
  std::string::size_type size = lowerCase.size();
  std::string::size_type pos = 0;
  for (; pos < size; ++pos) {
    if (std::isupper(lowerCase[pos])) {
      lowerCase[pos] = std::tolower(lowerCase[pos]);
    }
  }
  return lowerCase;
}

inline bool StartWith(const std::string& input, const std::string& pattern) {
  if (input.length() < pattern.length()) {
    return false;
  }

  size_t i = 0;
  while (i < pattern.length() && input[i] == pattern[i]) {
    i++;
  }

  return i == pattern.length();
};
}  // namespace storage_api
}  // namespace sdk
}  // namespace odps
}  // namespace apsara