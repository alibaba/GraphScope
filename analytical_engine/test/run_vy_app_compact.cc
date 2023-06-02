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

#include "apps/property/auto_sssp_property.h"
#include "apps/property/auto_wcc_property.h"
#include "apps/property/sssp_property.h"
#include "apps/property/wcc_property.h"
#include "bfs/bfs.h"
#include "bfs/bfs_auto.h"
#include "cdlp/cdlp.h"
#include "lcc/lcc.h"
#include "pagerank/pagerank.h"
#include "pagerank/pagerank_auto.h"
#include "sssp/sssp.h"
#include "wcc/wcc.h"
#include "wcc/wcc_auto.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"

using VertexMapType = vineyard::ArrowVertexMap<int64_t, uint64_t>;
using FragmentType =
    vineyard::ArrowFragment<int64_t, uint64_t, VertexMapType, true>;
using ProjectedFragmentType =
    gs::ArrowProjectedFragment<int64_t, uint64_t, int64_t, int64_t,
                               VertexMapType, true>;

void RunTraverse(std::shared_ptr<FragmentType> fragment,
                 const grape::CommSpec& comm_spec,
                 const std::string& out_prefix) {
  LOG(INFO) << "Run Traverse";

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);

  for (FragmentType::label_id_t v_label = 0;
       v_label != fragment->vertex_label_num(); ++v_label) {
    auto iv = fragment->InnerVertices(v_label);
    for (auto v : iv) {
      for (FragmentType::label_id_t e_label = 0;
           e_label != fragment->edge_label_num(); ++e_label) {
        auto oe = fragment->GetOutgoingAdjList(v, e_label);
        for (auto& e : oe) {
          ostream << fragment->GetId(v) << " " << fragment->GetId(e.neighbor())
                  << std::endl;
        }
      }
    }
  }

  ostream.close();
}

void RunWCC(std::shared_ptr<FragmentType> fragment,
            const grape::CommSpec& comm_spec, const std::string& out_prefix) {
  LOG(INFO) << "Run WCC";
  using AppType = gs::WCCProperty<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query();

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunSSSP(std::shared_ptr<FragmentType> fragment,
             const grape::CommSpec& comm_spec, const std::string& out_prefix) {
  LOG(INFO) << "Run SSSP";
  using AppType = gs::SSSPProperty<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query(4);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunAutoWCC(std::shared_ptr<FragmentType> fragment,
                const grape::CommSpec& comm_spec,
                const std::string& out_prefix) {
  LOG(INFO) << "Run Auto WCC";
  using AppType = gs::AutoWCCProperty<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query();

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunAutoSSSP(std::shared_ptr<FragmentType> fragment,
                 const grape::CommSpec& comm_spec,
                 const std::string& out_prefix) {
  LOG(INFO) << "Run Auto SSSP";
  using AppType = gs::AutoSSSPProperty<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query(4);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedTraverse(std::shared_ptr<ProjectedFragmentType> fragment,
                          const grape::CommSpec& comm_spec,
                          const std::string& out_prefix) {
  LOG(INFO) << "Run Projected Traverse";
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);

  for (auto const& v : fragment->InnerVertices()) {
    auto es = fragment->GetOutgoingAdjList(v);
    for (auto const& e : es) {
      ostream << fragment->GetId(v) << " " << fragment->GetId(e.neighbor())
              << " " << e.data() << std::endl;
    }
  }

  ostream.close();
}

void RunProjectedAutoWCC(std::shared_ptr<ProjectedFragmentType> fragment,
                         const grape::CommSpec& comm_spec,
                         const std::string& out_prefix) {
  LOG(INFO) << "Run Projected WCC";
  using AppType = grape::WCCAuto<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query();

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedAutoSSSP(std::shared_ptr<ProjectedFragmentType> fragment,
                          const grape::CommSpec& comm_spec,
                          const std::string& out_prefix) {
  LOG(INFO) << "Run Projected SSSP";
  using AppType = grape::SSSP<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query(6);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedCDLP(std::shared_ptr<ProjectedFragmentType> fragment,
                      const grape::CommSpec& comm_spec,
                      const std::string& out_prefix) {
  LOG(INFO) << "Run Projected CDLP";
  using AppType = grape::CDLP<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query(10);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedBFS(std::shared_ptr<ProjectedFragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix) {
  LOG(INFO) << "Run Projected BFS";
  using AppType = grape::BFSAuto<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query(6);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedLCC(std::shared_ptr<ProjectedFragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix) {
  LOG(INFO) << "Run Projected LCC";
  using AppType = grape::LCC<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query();

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void RunProjectedPR(std::shared_ptr<ProjectedFragmentType> fragment,
                    const grape::CommSpec& comm_spec,
                    const std::string& out_prefix) {
  LOG(INFO) << "Run Projected PR";
  using AppType = grape::PageRankAuto<ProjectedFragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query(0.85, 10);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();
}

void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id) {
  vineyard::ObjectMeta meta;
  VINEYARD_CHECK_OK(client.GetMetaData(id, meta));
  LOG(INFO) << "fragment type: " << meta.GetTypeName();

  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(id));

  RunTraverse(fragment, comm_spec, "./output_traverse/");
  RunWCC(fragment, comm_spec, "./output_wcc/");
  RunSSSP(fragment, comm_spec, "./output_sssp/");

  RunAutoWCC(fragment, comm_spec, "./output_auto_wcc/");
  RunAutoSSSP(fragment, comm_spec, "./output_auto_sssp/");

  std::shared_ptr<ProjectedFragmentType> projected_fragment =
      ProjectedFragmentType::Project(fragment, 0, 0, 0, 0);

  RunProjectedTraverse(projected_fragment, comm_spec,
                       "./output_projected_traverse/");
  RunProjectedAutoWCC(projected_fragment, comm_spec, "./output_projected_wcc/");
  RunProjectedAutoSSSP(projected_fragment, comm_spec,
                       "./output_projected_sssp/");
  RunProjectedCDLP(projected_fragment, comm_spec, "./output_projected_cdlp/");
  RunProjectedBFS(projected_fragment, comm_spec, "./output_projected_bfs/");
  RunProjectedLCC(projected_fragment, comm_spec, "./output_projected_lcc/");
  RunProjectedPR(projected_fragment, comm_spec, "./output_projected_pr/");
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./run_vy_app <ipc_socket> <e_label_num> <efiles...> "
        "<v_label_num> <vfiles...> <run_projected>"
        "[directed]\n");
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
  std::string app_name = "";
  std::string path_pattern = "";
  if (argc > index) {
    directed = atoi(argv[index++]);
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
      using oid_t = vineyard::property_graph_types::OID_TYPE;
      // using oid_t = std::string;
      using vid_t = vineyard::property_graph_types::VID_TYPE;
      using vertex_map_t =
          vineyard::ArrowVertexMap<typename vineyard::InternalType<oid_t>::type,
                                   vid_t>;
      using loader_t = gs::arrow_fragment_loader_t<oid_t, vid_t, vertex_map_t>;
      auto loader = std::make_unique<loader_t>(
          client, comm_spec, efiles, vfiles, directed != 0, false, false, true);
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
              << "] loaded graph to vineyard ... " << fragment_id;

    MPI_Barrier(comm_spec.comm());

    Run(client, comm_spec, fragment_id);

    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}

template class gs::ArrowProjectedFragment<int64_t, uint64_t, int64_t, int64_t,
                                          VertexMapType, true>;
