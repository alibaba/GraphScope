#include "glog/logging.h"

#include <filesystem>
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

void output_code_to_file(const std::string& code,
                         const std::string& output_file_path) {
  std::ofstream ofs(output_file_path);
  CHECK(ofs.is_open()) << "Failed to open file: " << output_file_path;
  ofs << code;
  ofs.close();
  LOG(INFO) << "Finish writing to: " << output_file_path;
}

int deserialze_plan_and_gen(const std::string& file_path,
                            const std::string& output_file_path) {
  // check file_path exists
  if (!std::filesystem::exists(file_path)) {
    LOG(ERROR) << "input file: [" << file_path << "] not found";
    return 1;
  }
  // check output_file_path exists
  if (std::filesystem::exists(output_file_path)) {
    LOG(WARNING) << "output file: [" << output_file_path
                 << "] exists, will overwrite";
  }

  LOG(INFO) << "Start deserializing from: " << file_path;
  std::string content_str = read_binary_str_from_path(file_path);
  LOG(INFO) << "Deserilized plan size : " << content_str.size() << ", from "
            << file_path;
  physical::PhysicalPlan plan_pb;
  auto stream = std::istringstream(content_str);
  CHECK(plan_pb.ParseFromArray(content_str.data(), content_str.size()));
  LOG(INFO) << "deserilized plan size : " << plan_pb.ByteSizeLong();
  LOG(INFO) << "deserilized plan : " << plan_pb.DebugString();
  BuildingContext context;
  QueryGenerator<uint8_t> query_generator(context, plan_pb);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Start writing to: " << output_file_path;
  output_code_to_file(res, output_file_path);
  return 0;
}

}  // namespace gs

// Usage: ./gen_query_from_pb <plan_file_path> <output_file_path>
int main(int argc, char** argv) {
  if (argc != 3) {
    LOG(INFO) << "Usage: " << argv[0] << " <plan_file_path> <output_file_path>";
    return 1;
  }
  std::string plan_file_path = argv[1];
  std::string output_file_path = argv[2];
  LOG(INFO) << "plan_file_path: " << plan_file_path;
  LOG(INFO) << "output_file_path: " << output_file_path;
  return gs::deserialze_plan_and_gen(plan_file_path, output_file_path);
}
