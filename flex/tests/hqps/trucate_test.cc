#include "flex/utils/property/column.h"

#include <string>
#include <string_view>

int main(int argc, char** argv) {
  std::string str = "abcdefO(1/ε^2)";
  std::cout << "str: " << str << ", size: " << str.size() << std::endl;
  std::string_view sv = gs::truncate_utf8(str, 12);
  std::cout << sv << ", size:" << sv.size() << std::endl;
  return 0;
}