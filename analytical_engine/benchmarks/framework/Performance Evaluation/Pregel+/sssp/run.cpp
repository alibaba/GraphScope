#include "pregel_app_sssp.h"

int main(int argc, char* argv[]) {
  string str = argv[1];
  init_workers();
  pregel_sssp(0, str, "/pregel+_data/pregel+-adj-outputest", true);
  worker_finalize();
  return 0;
}
