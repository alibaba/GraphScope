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

#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/utils/transform_utils.h"

using GraphType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;

using EmptyProjectedGraphType =
    gs::ArrowProjectedFragment<vineyard::property_graph_types::OID_TYPE,
                               vineyard::property_graph_types::VID_TYPE,
                               grape::EmptyType, grape::EmptyType>;

using EDProjectedGraphType =
    gs::ArrowProjectedFragment<vineyard::property_graph_types::OID_TYPE,
                               vineyard::property_graph_types::VID_TYPE,
                               grape::EmptyType, int64_t>;

template <typename APP_T, typename... Args>
void RunApp(std::shared_ptr<GraphType> fragment,
            const grape::CommSpec& comm_spec,
            const grape::ParallelEngineSpec& parallel_spec,
            const std::string& out_prefix, Args... args) {
  auto app = std::make_shared<APP_T>();

  auto worker = APP_T::CreateWorker(app, fragment);
  worker->Init(comm_spec, parallel_spec);
  double t0 = grape::GetCurrentTime();
  worker->Query(std::forward<Args>(args)...);
  double t1 = grape::GetCurrentTime();
  LOG(INFO) << "[worker-" << comm_spec.worker_id()
            << "]: Query time: " << t1 - t0;

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());
  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();
  worker->Finalize();
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./property_graph_loader <ipc_socket> <e_label_num> <efiles...> "
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
    fragment_id = boost::leaf::try_handle_all(
        [&loader]() { return loader->LoadFragment(); },
        [](const vineyard::GSError& e) {
          LOG(FATAL) << e.error_msg;
          return 0;
        },
        [](const boost::leaf::error_info& unmatched) {
          LOG(FATAL) << "Unmatched error " << unmatched;
          return 0;
        });
  }

  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "[property graph ids]:";
  }
  LOG(INFO) << "\n[frag-" << comm_spec.fid()
            << "]: " << vineyard::ObjectIDToString(fragment_id);

  MPI_Barrier(comm_spec.comm());

  std::shared_ptr<GraphType> fragment =
      std::dynamic_pointer_cast<GraphType>(client.GetObject(fragment_id));

  vineyard::ObjectID empty_frag_id =
      EmptyProjectedGraphType::Project(fragment, 0, -1, 0, -1)->id();
  vineyard::ObjectID ed_frag_id =
      EDProjectedGraphType::Project(fragment, 0, -1, 0, 0)->id();

  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "[empty graph ids]:";
  }
  LOG(INFO) << "\n[frag-" << comm_spec.fid()
            << "]: " << vineyard::ObjectIDToString(empty_frag_id);
  MPI_Barrier(comm_spec.comm());

  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "[ed graph ids]:";
  }
  LOG(INFO) << "\n[frag-" << comm_spec.fid()
            << "]: " << vineyard::ObjectIDToString(ed_frag_id);

  MPI_Barrier(comm_spec.comm());

  grape::FinalizeMPIComm();
  return 0;
}
