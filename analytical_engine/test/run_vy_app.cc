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
#include "bfs/bfs_opt.h"
#include "cdlp/cdlp.h"
#include "cdlp/cdlp_opt.h"
#include "lcc/lcc_opt.h"
#include "pagerank/pagerank_auto.h"
#include "pagerank/pagerank_local_parallel.h"
#include "pagerank/pagerank_opt.h"
#include "sssp/sssp_opt.h"
#include "wcc/wcc_opt.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/applications.h"

namespace bl = boost::leaf;

using oid_t = vineyard::property_graph_types::OID_TYPE;
using vid_t = vineyard::property_graph_types::VID_TYPE;

using FragmentType = vineyard::ArrowFragment<oid_t, vid_t>;


std::vector<int> prepareSamplingPathPattern(const std::string& path_pattern) {
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
  return label_id_seq;
}

void RunSamplingPath(std::shared_ptr<FragmentType> fragment,
                     const grape::CommSpec& comm_spec,
                     const std::string& out_prefix,
                     const std::string& path_pattern) {
  using AppType = gs::SamplingPath<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();

  std::vector<int> label_id_seq = prepareSamplingPathPattern(path_pattern);

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



void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id, bool run_projected, const std::string& app_name,
         const std::string& path_pattern) {
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(id));

  if (app_name == "lpa") {
  gs::RunPropertyApp(fragment, comm_spec, "./outputs_lpau2i/", "lpa_u2i");
  } else if (app_name == "sampling_path") {
    RunSamplingPath(fragment, comm_spec, "./outputs_sampling_path/",
                    path_pattern);
  } else {
    if (!run_projected) {
         gs::RunPropertyApp(fragment, comm_spec, "./outputs_wcc/", "wcc_property");
  gs::RunPropertyApp(fragment, comm_spec, "./outputs_sssp/", "sssp_property");

  gs::RunPropertyApp(fragment, comm_spec, "./outputs_auto_wcc/", "wcc_auto_property");
  gs::RunPropertyApp(fragment, comm_spec, "./outputs_auto_sssp/", "sssp_auto_property");
    } else {
  gs::RunProjectedApp(fragment, comm_spec, "./output_projected_wcc/", "wcc_projected");
  gs::RunProjectedApp(fragment, comm_spec, "./output_projected_sssp/", "sssp_projected");
  gs::RunProjectedApp(fragment, comm_spec, "./output_projected_cdlp/", "cdlp_projected");
  gs::RunProjectedApp(fragment, comm_spec, "./output_projected_bfs/", "bfs_projected");

  gs::RunProjectedApp(fragment, comm_spec, "./output_projected_lcc/", "lcc_projected");
  gs::RunProjectedApp(fragment, comm_spec, "./output_projected_pagerank/", "pagerank_projected");
  }
}}

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

    auto fragment_id = gs::LoadPropertyGraph(comm_spec, client, efiles, vfiles, directed != 0);

    Run(client, comm_spec, fragment_id, run_projected, app_name, path_pattern);
    LOG(INFO) << "memory: " << vineyard::get_rss_pretty()
              << ", peek memory: " << vineyard::get_peak_rss_pretty();

    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
