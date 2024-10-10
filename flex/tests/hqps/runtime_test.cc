#include "flex/engines/graph_db/runtime/adhoc/runtime.h"
#include <map>
#include <string>
#include <vector>

int main(int argc, char** argv) {
  physical::PhysicalPlan plan;
  std::map<std::string, std::string> params;

  // Receiving output
  std::vector<char> buf;
  gs::Encoder output(buf);

  gs::runtime::Context ctx;
  ctx.append_tag_id(0);
  LOG(INFO) << "Finish query, output size: " << buf.size();

  return 0;
}