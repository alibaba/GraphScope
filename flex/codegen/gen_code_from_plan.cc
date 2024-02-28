/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include <boost/program_options.hpp>

#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/json_util.h"

#include "flex/codegen/src/hqps_generator.h"
#include "flex/codegen/src/pegasus_generator.h"

namespace bpo = boost::program_options;

namespace gs {

std::string read_binary_str_from_path(const std::string& file_path) {
  std::ifstream ifs(file_path, std::ios::binary);
  CHECK(ifs.is_open()) << "Failed to open file: " << file_path;
  std::string content_str((std::istreambuf_iterator<char>(ifs)),
                          (std::istreambuf_iterator<char>()));
  return content_str;
}

std::string read_json_str_from_path(const std::string& file_path) {
  std::ifstream in(file_path);
  std::string input_json;
  in >> input_json;
  in.close();
  return input_json;
}

void output_code_to_file(const std::string& code,
                         const std::string& output_file_path) {
  std::ofstream ofs(output_file_path);
  CHECK(ofs.is_open()) << "Failed to open file: " << output_file_path;
  ofs << code;
  ofs.close();
  LOG(INFO) << "Finish writing to: " << output_file_path;
}
void deserialize_plan_and_gen_pegasus(const std::string& input_file_path,
                                      const std::string& output_file_path) {
  auto input_json = read_json_str_from_path(input_file_path);
  physical::PhysicalPlan plan;
  google::protobuf::util::JsonStringToMessage(input_json, &plan);
  gs::BuildingContext ctx;
  // parse query name from input_file_path
  std::string query_name =
      input_file_path.substr(input_file_path.find_last_of('/') + 1);
  gs::PegasusGenerator pegasus_generator(ctx, query_name, plan);
  std::string res = pegasus_generator.GenerateQuery();
  LOG(INFO) << "Start writing to: " << output_file_path;
  output_code_to_file(res, output_file_path);
}

void deserialize_plan_and_gen_hqps(const std::string& input_file_path,
                                   const std::string& output_file_path) {
  LOG(INFO) << "Start deserializing from: " << input_file_path;
  std::string content_str = read_binary_str_from_path(input_file_path);
  LOG(INFO) << "Deserialized plan size : " << content_str.size() << ", from "
            << input_file_path;
  physical::PhysicalPlan plan_pb;
  auto stream = std::istringstream(content_str);
  CHECK(plan_pb.ParseFromArray(content_str.data(), content_str.size()));
  LOG(INFO) << "deserialized plan size : " << plan_pb.ByteSizeLong();
  VLOG(1) << "deserialized plan : " << plan_pb.DebugString();
  BuildingContext context;
  QueryGenerator<uint8_t> query_generator(context, plan_pb);
  auto res = query_generator.GenerateQuery();
  LOG(INFO) << "Start writing to: " << output_file_path;
  output_code_to_file(res, output_file_path);
}
}  // namespace gs

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "engine,e", bpo::value<std::string>(), "engine type")(
      "input,i", bpo::value<std::string>(), "input plan path")(
      "output,o", bpo::value<std::string>(), "output file path");

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string input_path = vm["input"].as<std::string>();
  std::string output_path = vm["output"].as<std::string>();
  std::string engine_type = vm["engine"].as<std::string>();

  if (!std::filesystem::exists(input_path)) {
    LOG(ERROR) << "input file: [" << input_path << "] not found";
    return 1;
  }
  if (std::filesystem::exists(output_path)) {
    LOG(WARNING) << "output file: [" << output_path
                 << "] exists, will overwrite";
  }

  if (engine_type == "pegasus") {
    LOG(INFO) << "Start generating pegasus code";
    gs::deserialize_plan_and_gen_pegasus(input_path, output_path);
  } else if (engine_type == "hqps") {
    LOG(INFO) << "Start generating hqps code";
    gs::deserialize_plan_and_gen_hqps(input_path, output_path);
  } else {
    LOG(ERROR) << "Unknown engine type: " << engine_type
               << ", valid engine types: "
               << "<pegasus, hqps>";
    return 1;
  }

  LOG(INFO) << "Successfully generated code to " << output_path;

  return 0;
}
