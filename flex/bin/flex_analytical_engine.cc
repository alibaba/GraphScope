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

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "flex/engines/bsp/apps.h"
#include "flex/engines/bsp/bsp.h"
#include "flex/storages/immutable_graph/immutable_graph.h"

#include "grape/fragment/basic_fragment_loader.h"
#include "grape/fragment/loader.h"

DEFINE_string(application, "", "application name");
DEFINE_string(efile, "", "edge file");
DEFINE_string(vfile, "", "vertex file");
DEFINE_string(output_prefix, "", "output directory of results");

DEFINE_int64(bfs_source, 0, "source vertex of bfs.");
DEFINE_int32(cdlp_mr, 10, "max rounds of cdlp.");
DEFINE_int64(sssp_source, 0, "source vertex of sssp.");
DEFINE_double(pr_d, 0.85, "damping_factor of pagerank");
DEFINE_int32(pr_mr, 10, "max rounds of pagerank");

using WeightedGraph = immutable_graph::ImmutableGraph<
    int64_t, uint32_t, grape::EmptyType, double, grape::LoadStrategy::kOnlyOut,
    grape::GlobalVertexMap<int64_t, uint32_t,
                           grape::SegmentedPartitioner<int64_t>>>;
using NonWeightedGraph = immutable_graph::ImmutableGraph<
    int64_t, uint32_t, grape::EmptyType, grape::EmptyType,
    grape::LoadStrategy::kOnlyOut,
    grape::GlobalVertexMap<int64_t, uint32_t,
                           grape::SegmentedPartitioner<int64_t>>>;

#ifndef __AFFINITY__
#define __AFFINITY__ false
#endif

template <typename FRAG_T, typename APP_T, typename... Args>
void DoQuery(std::shared_ptr<FRAG_T> fragment, std::shared_ptr<APP_T> app,
             const grape::CommSpec& comm_spec, const std::string& out_prefix,
             Args... args) {
  auto spec = grape::MultiProcessSpec(comm_spec, __AFFINITY__);
  auto worker = APP_T::CreateWorker(app, fragment);
  worker->Init(comm_spec, spec);
  worker->Query(std::forward<Args>(args)...);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());
  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();
  worker->Finalize();
  VLOG(1) << "Worker-" << comm_spec.worker_id() << " finished: " << output_path;
}

int main(int argc, char** argv) {
  FLAGS_stderrthreshold = 0;
  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./analytical_engine [application _options]");
  if (argc == 1) {
    grape::gflags::ShowUsageWithFlagsRestrict(argv[0], "analytical_engine");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging("analytical_engine");
  google::InstallFailureSignalHandler();

  bsp::Init();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  std::string name = FLAGS_application;
  grape::LoadGraphSpec graph_spec = grape::DefaultLoadGraphSpec();
  auto out_prefix = FLAGS_output_prefix;
  if (name == "sssp") {
    auto fragment = grape::LoadGraph<WeightedGraph>(FLAGS_efile, FLAGS_vfile,
                                                    comm_spec, graph_spec);
    using AppType = bsp::SSSPApp<WeightedGraph>;
    auto app = std::make_shared<AppType>();

    DoQuery(fragment, app, comm_spec, out_prefix, FLAGS_sssp_source);
  } else {
    auto fragment = grape::LoadGraph<NonWeightedGraph>(FLAGS_efile, FLAGS_vfile,
                                                       comm_spec, graph_spec);
    if (name == "bfs") {
      using AppType = bsp::BFSApp<NonWeightedGraph>;
      auto app = std::make_shared<AppType>();

      DoQuery(fragment, app, comm_spec, out_prefix, FLAGS_bfs_source);
    } else if (name == "lcc") {
      using AppType = bsp::LCCApp<NonWeightedGraph>;
      auto app = std::make_shared<AppType>();

      DoQuery(fragment, app, comm_spec, out_prefix);
    } else if (name == "cdlp") {
      using AppType = bsp::CDLPApp<NonWeightedGraph>;
      auto app = std::make_shared<AppType>();

      DoQuery(fragment, app, comm_spec, out_prefix, FLAGS_cdlp_mr);
    } else if (name == "pagerank") {
      using AppType = bsp::PRApp<NonWeightedGraph>;
      auto app = std::make_shared<AppType>();

      DoQuery(fragment, app, comm_spec, out_prefix, FLAGS_pr_d, FLAGS_pr_mr);
    } else if (name == "wcc") {
      using AppType = bsp::WCCApp<NonWeightedGraph>;
      auto app = std::make_shared<AppType>();

      DoQuery(fragment, app, comm_spec, out_prefix);
    } else {
      LOG(FATAL) << "Invalid app: " << name;
    }
  }

  bsp::Finalize();
  google::ShutdownGoogleLogging();

  return 0;
}
