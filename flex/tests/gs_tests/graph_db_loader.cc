#include <string>
#include <iostream>

#include "flex/storages/mutable_csr/graph_db.h"

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cout << "Usage: " << argv[0] << " <yaml_path> <work_dir> <thread_num>" << std::endl;
    return 0;
  }
  std::string yaml_path = argv[1];
  std::string work_dir = argv[2];
  int thread_num = std::stoi(argv[3]);

  gs::GraphDB db;
  db.Init(yaml_path, work_dir, thread_num);

  return 0;
}