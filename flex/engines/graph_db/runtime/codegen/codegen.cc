#include <fstream>

#include "flex/engines/graph_db/runtime/codegen/builders/builders.h"

namespace gs {
namespace runtime {

class Codegen {
 public:
  Codegen(const physical::PhysicalPlan& plan) : plan_(plan) {}

  std::string Generate() {
    int opr_num = plan_.plan_size();
    std::stringstream ss;
    BuildingContext context;
    for (int i = 0; i < opr_num; i++) {
      const auto& opr = plan_.plan(i);
      CHECK(opr.has_opr()) << "Operator is not set in physical plan";
      switch (opr.opr().op_kind_case()) {
      case physical::PhysicalOpr_Operator::OpKindCase::kScan:
        ss << build_scan(context, opr.opr().scan());
        LOG(INFO) << ss.str();
        break;
      case physical::PhysicalOpr_Operator::OpKindCase::kSink:
        ss << build_sink(context);
        break;
      case physical::PhysicalOpr_Operator::OpKindCase::kLimit:
        ss << build_limit(context, opr.opr().limit());
        break;
      default:
        break;
      }
    }

    return ss.str();
  }
  const physical::PhysicalPlan& plan_;
};

}  // namespace runtime
}  // namespace gs

std::string read_pb(const std::string& filename) {
  std::ifstream file(filename, std::ios::binary);

  if (!file.is_open()) {
    LOG(FATAL) << "open pb file: " << filename << " failed...";
    return "";
  }

  file.seekg(0, std::ios::end);
  size_t size = file.tellg();
  file.seekg(0, std::ios::beg);

  std::string buffer;
  buffer.resize(size);

  file.read(&buffer[0], size);

  file.close();
  return buffer;
}

int main(int argc, char** argv) {
  char* filename = argv[1];
  physical::PhysicalPlan plan;
  plan.ParseFromString(read_pb(filename));

  gs::runtime::Codegen codegen(plan);
  codegen.Generate();
  return 0;
}