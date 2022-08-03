/**
 * Copyright 2020-2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdio.h>

#include <fstream>
#include <string>

#include "glog/logging.h"

#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/common/util/json.h"

#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/arrow_fragment_group.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"

#include "htap_ds_impl.h"

struct htap_loader_options {
  int edge_label_num;
  int vertex_label_num;
  std::vector<std::string> efiles;
  std::vector<std::string> vfiles;
  int directed;
  int generate_eid;
};

namespace detail {
bool parse_options_from_args(struct htap_loader_options &options, int current_index, int argc, char **argv) {
  options.edge_label_num = atoi(argv[current_index++]);
  for (int i = 0; i < options.edge_label_num; ++i) {
    options.efiles.push_back(argv[current_index++]);
  }

  options.vertex_label_num = atoi(argv[current_index++]);
  for (int i = 0; i < options.vertex_label_num; ++i) {
    options.vfiles.push_back(argv[current_index++]);
  }

  if (argc > current_index) {
    options.directed = atoi(argv[current_index++]);
  }
  if (argc > current_index) {
    options.generate_eid = atoi(argv[current_index++]);
  }
  return true;
}

bool parse_options_from_config_json(struct htap_loader_options &options, std::string const &config_json) {
  std::ifstream config_file(config_json);
  std::string config_json_content((std::istreambuf_iterator<char>(config_file)),
                                    std::istreambuf_iterator<char>());
  vineyard::json config = vineyard::json::parse(config_json_content);
  if (config.contains("vertices")) {
    for (auto const &item: config["vertices"]) {
      auto vfile = vineyard::ExpandEnvironmentVariables(item["data_path"].get<std::string>())
                 + "#label=" + item["label"].get<std::string>();
      if (item.contains("options")) {
        vfile += "#" + item["options"].get<std::string>();
      }
      options.vfiles.push_back(vfile);
    }
    options.vertex_label_num = options.vfiles.size();
  }
  if (config.contains("edges")) {
    for (auto const &item: config["edges"]) {
      auto efile = vineyard::ExpandEnvironmentVariables(item["data_path"].get<std::string>())
                 + "#label=" + item["label"].get<std::string>()
                 + "#src_label=" + item["src_label"].get<std::string>()
                 + "#dst_label=" + item["dst_label"].get<std::string>();
      if (item.contains("options")) {
        efile += "#" + item["options"].get<std::string>();
      }
      options.efiles.push_back(efile);
    }
  }
  if (config.contains("directed")) {
    if (config["directed"].is_boolean()) {
      options.directed = config["directed"].get<bool>();
    } else if (config["directed"].is_number_integer()) {
      options.directed = config["directed"].get<int>();
    } else {
      options.directed = config["directed"].get<std::string>() == "true";
    }
  }
  if (config.contains("generate_eid")) {
    if (config["generate_eid"].is_boolean()) {
      options.generate_eid = config["generate_eid"].get<bool>();
    } else if (config["generate_eid"].is_number_integer()) {
      options.generate_eid = config["generate_eid"].get<int>();
    } else {
      options.generate_eid = config["generate_eid"].get<std::string>() == "true";
    }
  }
  return true;
}

}

int main(int argc, char **argv) {
  if (argc < 3) {
    printf("usage: ./vineyard_htap_loader <e_label_num> <efiles...> <v_label_num> <vfiles...> [directed] [generate_eid]\n"
           "\n"
           "   or: ./vineyard_htap_loader --config <config.json>"
           "\n\n");
    return 1;
  }
  struct htap_loader_options options;
  if ((std::string(argv[1]) == "--config") || (std::string(argv[1]) == "-config" )) {
    if (!detail::parse_options_from_config_json(options, argv[2])) {
      exit(-1);
    }
  } else {
    if (!detail::parse_options_from_args(options, 1, argc, argv)) {
      exit(-1);
    }
  }

  grape::InitMPIComm();

  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client& client = vineyard::Client::Default();

    MPI_Barrier(comm_spec.comm());
    vineyard::ObjectID fragment_group_id;
    {
      auto loader = std::make_unique<vineyard::ArrowFragmentLoader<
          vineyard::property_graph_types::OID_TYPE,
          vineyard::property_graph_types::VID_TYPE>>(
            client, comm_spec, options.efiles, options.vfiles,
            options.directed != 0, options.generate_eid != 0);

      fragment_group_id = boost::leaf::try_handle_all(
          [&]() { return loader->LoadFragmentAsFragmentGroup(); },
          [](const boost::leaf::error_info &unmatched) {
            LOG(FATAL) << "Unmatched error " << unmatched;
            return 0;
          });
    }

    LOG(INFO) << "[fragment group id]: " << fragment_group_id;

    std::shared_ptr<vineyard::ArrowFragmentGroup> fg =
        std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
            client.GetObject(fragment_group_id));

    for (const auto &pair : fg->Fragments()) {
      LOG(INFO) << "[frag-" << pair.first << "]: " << pair.second;
    }

    vineyard::htap_impl::GraphHandleImpl *handle = new vineyard::htap_impl::GraphHandleImpl();
    vineyard::htap_impl::get_graph_handle(fragment_group_id, 1, handle);
    auto schema = handle->schema;
    if (comm_spec.worker_id() == 0) {
      LOG(INFO) << "schema = " << schema->ToJSONString();
    }
    schema->DumpToFile("/tmp/" + std::to_string(fragment_group_id) + ".json");
    LOG(INFO) << "The schema json has been dumped to '"
              << ("/tmp/" + std::to_string(fragment_group_id) + ".json")
              << "'";

    MPI_Barrier(comm_spec.comm());
  }
  grape::FinalizeMPIComm();

  return 0;
}
