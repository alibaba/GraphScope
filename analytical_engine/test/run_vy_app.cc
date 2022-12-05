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

#include "apps/lpa/lpa_u2i.h"
#include "apps/property/auto_sssp_property.h"
#include "apps/property/auto_wcc_property.h"
#include "apps/property/sssp_property.h"
#include "apps/property/wcc_property.h"
#include "apps/sampling_path/sampling_path.h"
#include "bfs/bfs.h"
#include "cdlp/cdlp.h"
#include "lcc/lcc.h"
#include "pagerank/pagerank.h"
#include "sssp/sssp.h"
#include "wcc/wcc.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"

namespace bl = boost::leaf;

using FragmentType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;

using ProjectedFragmentType =
    gs::ArrowProjectedFragment<int64_t, uint64_t, double, int64_t>;

void RunWCC(std::shared_ptr<FragmentType> fragment,
            const grape::CommSpec& comm_spec, const std::string& out_prefix) {
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

void RunLPAU2I(std::shared_ptr<FragmentType> fragment,
               const grape::CommSpec& comm_spec,
               const std::string& out_prefix) {
  using AppType = gs::LPAU2I<FragmentType>;
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

void RunSamplingPath(std::shared_ptr<FragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix,
                     const std::string& path_pattern) {
  using AppType = gs::SamplingPath<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();

  std::vector<int> label_id_seq;

  std::string delimiter = "-";
  auto start = 0U;
  auto end = path_pattern.find(delimiter);
  while (end != std::string::npos) {
    auto label = std::stoul(path_pattern.substr(start, end - start));
    label_id_seq.push_back(label);
    start = end + delimiter.length();
    end = path_pattern.find(delimiter, start);
  }
  label_id_seq.push_back(std::stoi(path_pattern.substr(start, end)));

  worker->Init(comm_spec, spec);
  worker->Query(label_id_seq, 10000000);

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

void RunProjectedWCC(std::shared_ptr<ProjectedFragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix) {
  /*
    using AppType =
        grape::WCCProjected<ProjectedFragmentType>;
    using AppType = grape::WCCAuto<ProjectedFragmentType>;
  */
  using AppType = grape::WCC<ProjectedFragmentType>;
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

void RunProjectedSSSP(std::shared_ptr<ProjectedFragmentType> fragment,
                      const grape::CommSpec& comm_spec,
                      const std::string& out_prefix) {
  /*
    using AppType =
        grape::SSSPProjected<ProjectedFragmentType>;
    using AppType = grape::SSSPAuto<ProjectedFragmentType>;
  */
  using AppType = grape::SSSP<ProjectedFragmentType>;
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

void RunProjectedCDLP(std::shared_ptr<ProjectedFragmentType> fragment,
                      const grape::CommSpec& comm_spec,
                      const std::string& out_prefix) {
  // using AppType = grape::CDLPAuto<ProjectedFragmentType>;
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
  // using AppType = grape::BFSAuto<ProjectedFragmentType>;
  using AppType = grape::BFS<ProjectedFragmentType>;
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

void RunProjectedLCC(std::shared_ptr<ProjectedFragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix) {
  // using AppType = grape::LCCAuto<ProjectedFragmentType>;
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
  // using AppType = grape::PageRankAuto<ProjectedFragmentType>;
  using AppType = grape::PageRank<ProjectedFragmentType>;
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
         vineyard::ObjectID id, bool run_projected, const std::string& app_name,
         const std::string& path_pattern) {
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(id));

  if (app_name == "lpa") {
    RunLPAU2I(fragment, comm_spec, "./outputs_lpau2i/");
  } else if (app_name == "sampling_path") {
    RunSamplingPath(fragment, comm_spec, "./outputs_sampling_path/",
                    path_pattern);
  } else {
    if (!run_projected) {
      RunWCC(fragment, comm_spec, "./outputs_wcc/");
      RunSSSP(fragment, comm_spec, "./outputs_sssp/");

      RunAutoWCC(fragment, comm_spec, "./outputs_auto_wcc/");
      RunAutoSSSP(fragment, comm_spec, "./outputs_auto_sssp/");
    } else {
      std::shared_ptr<ProjectedFragmentType> projected_fragment =
          ProjectedFragmentType::Project(fragment, 0, 0, 0, 0);

      RunProjectedWCC(projected_fragment, comm_spec, "./output_projected_wcc/");
      RunProjectedSSSP(projected_fragment, comm_spec,
                       "./output_projected_sssp/");
      RunProjectedCDLP(projected_fragment, comm_spec,
                       "./output_projected_cdlp/");
      RunProjectedBFS(projected_fragment, comm_spec, "./output_projected_bfs/");
      RunProjectedLCC(projected_fragment, comm_spec, "./output_projected_lcc/");
      RunProjectedPR(projected_fragment, comm_spec, "./output_projected_pr/");
    }
  }
}

int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./run_vy_app <ipc_socket> <e_label_num> <efiles...> "
        "<v_label_num> <vfiles...> <run_projected>"
        "[directed] [app_name] [path_pattern]\n");
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

  int run_projected = atoi(argv[index++]);

  int directed = 1;
  std::string app_name = "";
  std::string path_pattern = "";
  if (argc > index) {
    directed = atoi(argv[index++]);
  }
  if (argc > index) {
    app_name = argv[index++];
  }
  if (argc > index) {
    path_pattern = argv[index++];
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
      using vid_t = vineyard::property_graph_types::VID_TYPE;

      auto loader = std::make_unique<gs::ArrowFragmentLoader<oid_t, vid_t>>(
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
              << "] loaded graph to vineyard ... ";

    MPI_Barrier(comm_spec.comm());

    Run(client, comm_spec, fragment_id, run_projected, app_name, path_pattern);

    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}

template class gs::ArrowProjectedFragment<int64_t, uint64_t, double, int64_t>;
