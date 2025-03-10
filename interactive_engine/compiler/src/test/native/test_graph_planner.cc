
#include <graph_planner.h>
#include <string>

// Just a simple test to check if the GraphPlannerWrapper can be constructed
int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <java class path> <jna lib path>"
              << std::endl;
    return 1;
  }
  std::string java_path = argv[1];
  std::string jna_path = argv[2];
  gs::GraphPlannerWrapper gpw(java_path, jna_path);
  std::cout << "Success" << std::endl;
  return 0;
}