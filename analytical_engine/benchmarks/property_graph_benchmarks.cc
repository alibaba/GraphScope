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

#include "benchmarks/apps/bfs/property_bfs.h"
#include "benchmarks/apps/pagerank/property_pagerank.h"
#include "benchmarks/apps/sssp/property_sssp.h"
#include "benchmarks/apps/wcc/property_wcc.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/utils/transform_utils.h"

using GraphType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;

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
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  int basic_argc = comm_spec.fnum() + 3;

  if (argc < basic_argc) {
    printf(
        "usage: ./property_graph_benchmarks <ipc_socket> <app> <frag_0> ... "
        "<frag_n-1> [query_args]\n");
    return 1;
  }

  std::string ipc_socket = std::string(argv[1]);
  std::string app_name = argv[2];
  std::string frag_id_str = argv[3 + comm_spec.fid()];

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

  vineyard::ObjectID fragment_id = vineyard::ObjectIDFromString(frag_id_str);

  MPI_Barrier(comm_spec.comm());

  std::shared_ptr<GraphType> fragment =
      std::dynamic_pointer_cast<GraphType>(client.GetObject(fragment_id));

  auto parallel_spec = grape::DefaultParallelEngineSpec();

  if (app_name == "bfs") {
    CHECK_GE(argc, basic_argc + 1);
    std::string root = argv[basic_argc];
    RunApp<gs::benchmarks::PropertyBFS<GraphType>>(
        fragment, comm_spec, parallel_spec, "./output_pp_bfs/",
        boost::lexical_cast<vineyard::property_graph_types::OID_TYPE>(root));
  } else if (app_name == "sssp") {
    CHECK_GE(argc, basic_argc + 1);
    std::string root = argv[basic_argc];
    RunApp<gs::benchmarks::PropertySSSP<GraphType>>(
        fragment, comm_spec, parallel_spec, "./output_pp_sssp/",
        boost::lexical_cast<vineyard::property_graph_types::OID_TYPE>(root));
  } else if (app_name == "wcc") {
    RunApp<gs::benchmarks::PropertyWCC<GraphType>>(
        fragment, comm_spec, parallel_spec, "./output_pp_wcc/");
  } else if (app_name == "pr") {
    CHECK_GE(argc, basic_argc + 2);
    std::string delta = argv[basic_argc];
    std::string max_round = argv[basic_argc + 1];
    RunApp<gs::benchmarks::PropertyPageRank<GraphType>>(
        fragment, comm_spec, parallel_spec, "./output_pp_pr/",
        boost::lexical_cast<double>(delta),
        boost::lexical_cast<int>(max_round));
  }

  MPI_Barrier(comm_spec.comm());

  grape::FinalizeMPIComm();
  return 0;
}
