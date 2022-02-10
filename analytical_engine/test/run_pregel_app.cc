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

#include <sys/stat.h>
#include <cstdio>
#include <fstream>
#include <string>

#include "glog/logging.h"

#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/fragment/loader.h"
#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "apps/pregel/aggregators_test.h"
#include "apps/pregel/pagerank_pregel.h"
#include "apps/pregel/sssp_pregel.h"
#include "apps/pregel/tc_pregel.h"
#include "core/app/pregel/pregel_app_base.h"
#include "core/loader/arrow_fragment_loader.h"

using FragmentType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;

template <typename FRAG_T, typename APP_T>
void RunPregelApp(std::shared_ptr<FRAG_T> fragment,
                  const grape::CommSpec& comm_spec, const std::string& query,
                  const std::string& out_prefix) {
  auto app = std::make_shared<APP_T>();
  auto worker = APP_T::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query(query);

  if (access(out_prefix.c_str(), 0) != 0) {
    mkdir(out_prefix.c_str(), 0777);
  }

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
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(id));

  RunPregelApp<FragmentType,
               gs::PregelPropertyAppBase<FragmentType, gs::PregelPagerank,
                                         gs::PregelPagerankCombinator>>(
      fragment, comm_spec, "{\"delta\": 4, \"max_round\": 10}",
      "./outputs_pregel_pr_with_combinator");
  RunPregelApp<FragmentType,
               gs::PregelPropertyAppBase<FragmentType, gs::PregelPagerank>>(
      fragment, comm_spec, "{\"delta\": 4, \"max_round\": 10}",
      "./outputs_pregel_pr");
  RunPregelApp<FragmentType,
               gs::PregelPropertyAppBase<FragmentType, gs::PregelSSSP>>(
      fragment, comm_spec, "{\"src\": 4}", "./outputs_pregel_sssp");
  RunPregelApp<FragmentType,
               gs::PregelPropertyAppBase<FragmentType, gs::AggregatorsTest>>(
      fragment, comm_spec, "{}", "./pregel_aggregator_test");
}

void RunTC(grape::CommSpec const& comm_spec, std::string& efile,
           std::string& vfile, const std::string& query,
           std::string& output_prefix) {
  using VertexMapType = grape::GlobalVertexMap<int64_t, uint32_t,
                                               grape::SegmentedPartitioner<int64_t>>;
  using GraphType =
      grape::ImmutableEdgecutFragment<int64_t, uint32_t, grape::EmptyType,
                                      grape::EmptyType,
                                      grape::LoadStrategy::kBothOutIn,
                                      VertexMapType>;
  using AppType = gs::PregelAppBase<GraphType, gs::PregelTC<GraphType>>;
  auto load_spec = grape::DefaultLoadGraphSpec();

  load_spec.set_directed(false);

  auto fragment =
      grape::LoadGraph<GraphType>(efile, vfile, comm_spec, load_spec);

  RunPregelApp<GraphType, AppType>(fragment, comm_spec, query, output_prefix);
}

int main(int argc, char** argv) {
  if (argc != 5 && argc < 7) {
    printf("usage: ./run_pregel_app tc <efile> <vfile> <output_prefix>\n");
    printf(
        "usage: ./run_pregel_app <ipc_socket> <e_label_num> <efiles...> "
        "<v_label_num> <vfiles...> [directed]\n");
    return 1;
  }

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    std::string app_name = std::string(argv[1]);
    if (app_name == "tc") {
      std::string efile = argv[2], vfile = argv[3], output_prefix = argv[4];
      RunTC(comm_spec, efile, vfile, "", output_prefix);
    } else {
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

      LOG(INFO) << "[worker-" << comm_spec.worker_id()
                << "] loaded graph to vineyard ...";

      MPI_Barrier(comm_spec.comm());

      // RunSSSP(client, comm_spec, fragment_id, "{\"src\": 4}");
      Run(client, comm_spec, fragment_id);

      MPI_Barrier(comm_spec.comm());
    }
  }

  grape::FinalizeMPIComm();
  return 0;
}
