#include "glog/logging.h"

#include "proto_generated_gie/common.pb.h"
#include "proto_generated_gie/physical.pb.h"

#include "flex/codegen/query_generator.h"

namespace gs {

std::string read_binary_str_from_path(const std::string& file_path) {
  std::ifstream ifs(file_path, std::ios::binary);
  CHECK(ifs.is_open()) << "Failed to open file: " << file_path;
  std::string content_str((std::istreambuf_iterator<char>(ifs)),
                          (std::istreambuf_iterator<char>()));
  return content_str;
}

void test_deserialze_plan_and_gen(const std::string& file_path) {
  LOG(INFO) << "Start deserializing from: " << file_path;
  std::string content_str = read_binary_str_from_path(file_path);
  LOG(INFO) << "deserilized plan size : " << content_str.size() << ", from "
            << file_path;
  physical::PhysicalPlan plan_pb;
  auto stream = std::istringstream(content_str);
  CHECK(plan_pb.ParseFromArray(content_str.data(), content_str.size()));
  LOG(INFO) << "deserilized plan size : " << plan_pb.ByteSizeLong();
  LOG(INFO) << "deserilized plan : " << plan_pb.DebugString();
  BuildingContext context;
  QueryGenerator<uint8_t> query_generator(context, plan_pb);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "generated plan: " << std::endl << res;
}

}  // namespace gs

// Usage: ./deserialize_and_gen <plan_file_path>
// will output plan to stderr
int main(int argc, char** argv) {
  CHECK(argc == 2);
  std::string plan_file_path = argv[1];
  gs::test_deserialze_plan_and_gen(plan_file_path);
  return 0;
}
