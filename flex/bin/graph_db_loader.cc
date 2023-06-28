#include <iostream>
#include <string>

#include "grape/util.h"

#include "flex/engines/graph_db/database/graph_db.h"

#include <boost/program_options.hpp>

#include "glog/logging.h"

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "bulk-load,l", bpo::value<std::string>(), "bulk-load config file");
  google::InitGoogleLogging(argv[0]);

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string graph_schema_path = "";
  std::string data_path = "";
  std::string bulk_load_config_path = "";

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  if (vm.count("bulk-load")) {
    bulk_load_config_path = vm["bulk-load"].as<std::string>();
  }

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto ret = gs::Schema::LoadFromYaml(graph_schema_path, bulk_load_config_path);
  db.Init(std::get<0>(ret), std::get<1>(ret), std::get<2>(ret),
          std::get<3>(ret), data_path, 1);

  t0 += grape::GetCurrentTime();

  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  return 0;
}