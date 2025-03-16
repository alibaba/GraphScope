#include "pregel_app_pagerank.h"

int main(int argc, char* argv[]) {
  string str = argv[1];
  cout << str << endl;
  init_workers();
  pregel_pagerank(str, "/pregel+_data/pregel+-adj-3600000_output", true);
  worker_finalize();
  return 0;
}
