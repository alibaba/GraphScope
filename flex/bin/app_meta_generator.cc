/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/program_options.hpp>

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "flex/engines/graph_db/app/app_base.h"

namespace bpo = boost::program_options;

void load_and_generate_meta(const std::string input_lib_path,
                            const std::string output_meta_path) {
  // Load the dynamic library
  gs::SharedLibraryAppFactory factor(input_lib_path);
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "input,i", bpo::value<std::string>(), "path to the input dynamic lib")(
      "output,o", bpo::value<std::string>(), "path to the output meta file");

  bpo::variables_map vm;
  bpo::store(bpo::parse_command_line(argc, argv, desc), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  if (!vm.count("input")) {
    std::cerr << "Please specify the input dynamic lib" << std::endl;
    return -1;
  }

  if (!vm.count("output")) {
    std::cerr << "Please specify the output meta file" << std::endl;
    return -1;
  }

  std::string input = vm["input"].as<std::string>();
  std::string output = vm["output"].as<std::string>();
  load_and_generate_meta(input, output);
  return 0;
}