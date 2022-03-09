/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <cstdio>
#include <fstream>
#include <string>

#include "boost/lexical_cast.hpp"
#include "glog/logging.h"

#include "grape/fragment/ev_fragment_loader.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/fragment/loader.h"
#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "benchmarks/apps/bfs/bfs.h"
#include "benchmarks/apps/pagerank/pagerank.h"
#include "benchmarks/apps/sssp/sssp.h"
#include "benchmarks/apps/wcc/wcc.h"

using EmptyGraphType =
    grape::ImmutableEdgecutFragment<vineyard::property_graph_types::OID_TYPE,
                                    vineyard::property_graph_types::VID_TYPE,
                                    grape::EmptyType, grape::EmptyType,
                                    grape::LoadStrategy::kBothOutIn>;

using EDGraphType =
    grape::ImmutableEdgecutFragment<vineyard::property_graph_types::OID_TYPE,
                                    vineyard::property_graph_types::VID_TYPE,
                                    grape::EmptyType, int64_t,
                                    grape::LoadStrategy::kBothOutIn>;

template <typename GRAPH_T, typename APP_T, typename... Args>
void LoadAndRunApp(const grape::CommSpec& comm_spec, const std::string& efile,
                   const std::string& vfile, bool directed,
                   const grape::ParallelEngineSpec& parallel_spec,
                   const std::string& serial_prefix,
                   const std::string& out_prefix, Args... args) {
  grape::LoadGraphSpec graph_spec = grape::DefaultLoadGraphSpec();
  graph_spec.set_directed(directed);
  graph_spec.set_deserialize(true, serial_prefix);
  graph_spec.set_rebalance(false, 0);

  std::shared_ptr<GRAPH_T> fragment;
  fragment = grape::LoadGraph<GRAPH_T>(efile, vfile, comm_spec, graph_spec);

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
  if (argc < 5) {
    printf(
        "usage: ./basic_graph_benchmarks <efile> <vfile> <directed> <app> "
        "<serialization_prefix> "
        "[query_args]\n");
    return 1;
  }

  std::string epath = argv[1];
  std::string vpath = argv[2];
  int directed = atoi(argv[3]);

  std::string app_name = argv[4];
  std::string serialization_prefix = argv[5];

  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  auto parallel_spec = grape::DefaultParallelEngineSpec();

  if (app_name == "sssp") {
    CHECK_GE(argc, 7);
    std::string root = argv[6];

    LoadAndRunApp<EDGraphType, gs::benchmarks::SSSP<EDGraphType>>(
        comm_spec, epath, vpath, directed, parallel_spec, serialization_prefix,
        "./output_or_sssp",
        boost::lexical_cast<vineyard::property_graph_types::OID_TYPE>(root));
  } else if (app_name == "bfs") {
    CHECK_GE(argc, 7);
    std::string root = argv[6];

    LoadAndRunApp<EmptyGraphType, gs::benchmarks::BFS<EmptyGraphType>>(
        comm_spec, epath, vpath, directed, parallel_spec, serialization_prefix,
        "./output_or_bfs",
        boost::lexical_cast<vineyard::property_graph_types::OID_TYPE>(root));
  } else if (app_name == "wcc") {
    LoadAndRunApp<EmptyGraphType, gs::benchmarks::WCC<EmptyGraphType>>(
        comm_spec, epath, vpath, directed, parallel_spec, serialization_prefix,
        "./output_or_wcc");
  } else if (app_name == "pr") {
    CHECK_GE(argc, 8);
    std::string delta = argv[6];
    std::string max_round = argv[7];

    LoadAndRunApp<EmptyGraphType, gs::benchmarks::PageRank<EmptyGraphType>>(
        comm_spec, epath, vpath, directed, parallel_spec, serialization_prefix,
        "./output_or_pr", boost::lexical_cast<double>(delta),
        boost::lexical_cast<int>(max_round));
  }

  MPI_Barrier(comm_spec.comm());

  grape::FinalizeMPIComm();
  return 0;
}
