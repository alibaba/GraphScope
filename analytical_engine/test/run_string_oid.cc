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

#include <stdio.h>
#include <fstream>
#include <string>

#include "glog/logging.h"

#include "vineyard/client/client.h"

#include "grape/grape.h"
#include "grape/util.h"
#include "sssp/sssp.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

namespace bl = boost::leaf;

using FragmentType = vineyard::ArrowFragment<std::string, uint64_t>;
using ProjectedFragmentType =
    gs::ArrowProjectedFragment<std::string, uint64_t, double, int64_t>;

void RunCTXSSSP(std::shared_ptr<ProjectedFragmentType> fragment,
                const grape::CommSpec& comm_spec,
                const std::string& out_prefix) {
  using AppType = grape::SSSP<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  worker->Query("v4");

  auto ctx = worker->GetContext();

  worker->Finalize();

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());
  LOG(INFO) << "to write to: " << output_path;
  ostream.open(output_path);
  ctx->Output(ostream);
  ostream.close();
}

void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id) {
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(id));
  std::shared_ptr<ProjectedFragmentType> projected_fragment =
      ProjectedFragmentType::Project(fragment, 0, 0, 0, 0);

  RunCTXSSSP(projected_fragment, comm_spec, "./output_string_oid_sssp/");
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./run_string_oid <ipc_socket> <e_label_num> <efiles...> "
        "<v_label_num> <vfiles...> [directed]\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

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
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

    vineyard::ObjectID fragment_id;
    {
      auto loader = std::make_unique<
          gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                                  vineyard::property_graph_types::VID_TYPE>>(
          client, comm_spec, efiles, vfiles, directed != 0);
      fragment_id =
          bl::try_handle_all([&loader]() { return loader->LoadFragment(); },
                             [](const vineyard::GSError& e) {
                               LOG(FATAL) << e.error_msg;
                               return 0;
                             },
                             [](const bl::error_info& unmatched) {
                               LOG(FATAL) << "Unmatched error " << unmatched;
                               return 0;
                             });
    }

    LOG(INFO) << "[worker-" << comm_spec.worker_id()
              << "] loaded graph to vineyard ...";

    MPI_Barrier(comm_spec.comm());

    Run(client, comm_spec, fragment_id);

    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
