#include "pregel_app_betweenness.h"

int main(int argc, char* argv[]) {
  string str = argv[1];
  cout << str << endl;
  init_workers();
  pregel_betweenness(str, "/pregel+_data/toy_output");
  cout << "finish" << endl;
  worker_finalize();
  return 0;
}
