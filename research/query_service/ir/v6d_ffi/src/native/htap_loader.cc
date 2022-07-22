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

#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/arrow_fragment_group.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"

#include "htap_ds_impl.h"

int main(int argc, char **argv) {
  if (argc < 6) {
    printf("usage: ./htap_loader <e_label_num> <efiles...> "
           "<v_label_num> <vfiles...> [directed]\n");
    return 1;
  }
  int index = 1;
  int edge_label_num = atoi(argv[index++]);
  std::vector<std::string> efiles;
  for (int i = 0; i < edge_label_num; ++i) {
    efiles.push_back(argv[index++]);
  }

  int vertex_label_num = atoi(argv[index++]);
  std::vector<std::string> vfiles;
  for (int i = 0; i < vertex_label_num; ++i) {
    vfiles.push_back(argv[index++]);
  }

  int directed = 1;
  if (argc > index) {
    directed = atoi(argv[index]);
  }

  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  vineyard::Client& client = vineyard::Client::Default();

  MPI_Barrier(comm_spec.comm());
  vineyard::ObjectID fragment_group_id;
  {
    auto loader = std::make_unique<vineyard::ArrowFragmentLoader<
        vineyard::property_graph_types::OID_TYPE,
        vineyard::property_graph_types::VID_TYPE>>(client, comm_spec, efiles,
                                                   vfiles, directed != 0);

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

  MPI_Barrier(comm_spec.comm());

  grape::FinalizeMPIComm();

  return 0;
}
