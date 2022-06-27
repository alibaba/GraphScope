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

#include <cstdio>
#include <fstream>
#include <string>

#include "glog/logging.h"

#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
/*
#include "bfs/bfs.h"
#include "cdlp/cdlp.h"
#include "lcc/lcc.h"
#include "pagerank/pagerank.h"
#include "sssp/sssp.h"
#include "wcc/wcc.h"
*/

// #include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"

namespace bl = boost::leaf;

/*
using FragmentType =
    gs::ArrowProjectedFragment<int64_t, uint64_t, int64_t, double>;

void RunProjectedWCC(std::shared_ptr<FragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix) {
  using AppType = grape::WCC<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  auto t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Start to WCC Query";
  }
  worker->Query();
  MPI_Barrier(comm_spec.comm());
  auto t2 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Projected WCC:" << t2 - t1 << "s";
  }
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedSSSP(std::shared_ptr<FragmentType> fragment,
                      const grape::CommSpec& comm_spec,
                      const std::string& out_prefix) {
  using AppType = grape::SSSP<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  auto t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Start to SSSP Query";
  }
  // worker->Query(6);
  // worker->Query(62455266);
  // worker->Query(420);
  worker->Query(101);
  auto t2 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Projected SSSP:" << t2 - t1 << "s";
  }

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedCDLP(std::shared_ptr<FragmentType> fragment,
                      const grape::CommSpec& comm_spec,
                      const std::string& out_prefix) {
  using AppType = grape::CDLP<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  auto t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Start to CDLP Query";
  }
  worker->Query(10);
  auto t2 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Projected CDLP:" << t2 - t1 << "s";
  }
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedBFS(std::shared_ptr<FragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix) {
  using AppType = grape::BFS<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  auto t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Start to BFS Query";
  }
  // worker->Query(6);
  // worker->Query(62455266);
  // worker->Query(420);
  worker->Query(101);
  auto t2 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Projected BFS:" << t2 - t1 << "s";
  }

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedLCC(std::shared_ptr<FragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix) {
  using AppType = grape::LCC<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  auto t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Start to LCC Query";
  }
  worker->Query();
  auto t2 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Projected LCC:" << t2 - t1 << "s";
  }

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedPR(std::shared_ptr<FragmentType> fragment,
                    const grape::CommSpec& comm_spec,
                    const std::string& out_prefix) {
  using AppType = grape::PageRank<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  MPI_Barrier(comm_spec.comm());
  auto t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Start to PR Query";
  }
  worker->Query(0.85, 10);
  auto t2 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(ERROR) << "Projected PR:" << t2 - t1 << "s";
  }
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}
*/

void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id) {
  using GraphType =
      vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;

  std::shared_ptr<GraphType> fragment =
      std::dynamic_pointer_cast<GraphType>(client.GetObject(id));
  MPI_Barrier(comm_spec.comm());
  /*
  std::shared_ptr<FragmentType> projected_fragment =
      FragmentType::Project(fragment, 0, 0, 0, 0);

  RunProjectedWCC(projected_fragment, comm_spec, "/mnt2/output_projected_wcc/");
  RunProjectedLCC(projected_fragment, comm_spec, "/mnt2/output_projected_lcc/");
  RunProjectedPR(projected_fragment, comm_spec, "/mnt2/output_projected_pr/");
  RunProjectedCDLP(projected_fragment, comm_spec,
                   "/mnt2/output_projected_cdlp/");
  RunProjectedSSSP(projected_fragment, comm_spec,
                   "/mnt2/output_projected_sssp/");
  RunProjectedBFS(projected_fragment, comm_spec, "/mnt2/output_projected_bfs/");
  */
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./run_vy_ldbc <ipc_socket> <e_label_num> <efiles...> "
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
          // gs::ArrowFragmentLoader<std::string,
          gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                                  // gs::ArrowFragmentLoader<std::string,
                                  vineyard::property_graph_types::VID_TYPE>>(
          client, comm_spec, efiles, vfiles, directed != 0);
      fragment_id = bl::try_handle_all(
          [&loader]() { return loader->LoadFragmentWithLocalVertexMap(); },
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
