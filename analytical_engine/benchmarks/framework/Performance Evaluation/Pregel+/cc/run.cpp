#include "pregel_app_sv.h"

int main(int argc, char* argv[]) {
  string str = argv[1];
  init_workers();
  pregel_sv(str, "/pregel+_data/pregel+-adj-3600000_output");
  worker_finalize();
  return 0;
}