#include "pregel_app_lpa.h"

int main(int argc, char* argv[]) {
  string str = argv[1];
  cout << str << endl;
  init_workers();
  pregel_lpa(str, "/pregel+_data/toy_output");
  worker_finalize();
  return 0;
}
