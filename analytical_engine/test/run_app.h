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

#ifndef ANALYTICAL_ENGINE_TEST_RUN_APP_H_
#define ANALYTICAL_ENGINE_TEST_RUN_APP_H_

#include <sys/stat.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"

#include "grape/config.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/fragment/loader.h"
#include "grape/grape.h"

#include "bfs/bfs.h"
#include "bfs/bfs_auto.h"
#include "cdlp/cdlp.h"
#include "cdlp/cdlp_auto.h"
#include "lcc/lcc.h"
#include "lcc/lcc_auto.h"
#include "pagerank/pagerank.h"
#include "pagerank/pagerank_auto.h"
#include "sssp/sssp.h"
#include "sssp/sssp_auto.h"
#include "wcc/wcc.h"
#include "wcc/wcc_auto.h"

#include "apps/bfs/bfs_generic.h"
#include "apps/centrality/degree/degree_centrality.h"
// #include "apps/centrality/eigenvector/eigenvector_centrality.h"
// #include "apps/centrality/katz/katz_centrality.h"
#include "apps/clustering/avg_clustering.h"
#include "apps/clustering/clustering.h"
#include "apps/clustering/transitivity.h"
#include "apps/clustering/triangles.h"
#include "apps/dfs/dfs.h"
#include "apps/hits/hits.h"
#include "apps/kcore/kcore.h"
#include "apps/kshell/kshell.h"
#include "apps/sssp/sssp_average_length.h"
#include "apps/sssp/sssp_has_path.h"
#include "apps/sssp/sssp_path.h"
#include "core/flags.h"
#include "core/fragment/dynamic_fragment.h"

DECLARE_string(application);
DECLARE_bool(directed);
DECLARE_string(efile);
DECLARE_string(vfile);
DECLARE_string(out_prefix);
DECLARE_string(datasource);
DECLARE_string(jobid);

DECLARE_int64(bfs_source);
DECLARE_string(degree_centrality_type);

DECLARE_double(eigenvector_centrality_tolerance);
DECLARE_int32(eigenvector_centrality_max_round);

DECLARE_double(hits_tolerance);
DECLARE_int32(hits_max_round);
DECLARE_bool(hits_normalized);

DECLARE_int32(kcore_k);

DECLARE_int32(kshell_k);

DECLARE_double(katz_centrality_alpha);
DECLARE_double(katz_centrality_beta);
DECLARE_double(katz_centrality_tolerance);
DECLARE_int32(katz_centrality_max_round);
DECLARE_bool(katz_centrality_normalized);

DECLARE_int64(sssp_source);
DECLARE_int64(sssp_target);
DECLARE_bool(sssp_weight);

DECLARE_int64(bfs_source);
DECLARE_int32(bfs_depth_limit);
DECLARE_string(bfs_output_format);

DECLARE_bool(segmented_partition);
DECLARE_bool(rebalance);
DECLARE_int32(rebalance_vertex_factor);

DECLARE_bool(serialize);
DECLARE_bool(deserialize);
DECLARE_string(serialization_prefix);

DECLARE_int32(app_concurrency);

DECLARE_int64(dfs_source);
DECLARE_string(dfs_format);

namespace gs {

void Init() {
  if (FLAGS_out_prefix.empty()) {
    LOG(FATAL) << "Please assign an output prefix.";
  }
  if (FLAGS_deserialize && FLAGS_serialization_prefix.empty()) {
    LOG(FATAL) << "Please assign a serialization prefix.";
  } else if (FLAGS_vfile.empty() || FLAGS_efile.empty()) {
    LOG(FATAL) << "Please assign input vertex/edge files.";
  }

  if (access(FLAGS_out_prefix.c_str(), 0) != 0) {
    mkdir(FLAGS_out_prefix.c_str(), 0777);
  }

  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}

template <typename FRAG_T, typename APP_T, typename... Args>
void CreateAndQuery(const grape::CommSpec& comm_spec, const std::string efile,
                    const std::string& vfile, const std::string& out_prefix,
                    const std::string& datasource, int fnum,
                    const grape::ParallelEngineSpec& spec, Args... args) {
  grape::LoadGraphSpec graph_spec = grape::DefaultLoadGraphSpec();
  graph_spec.set_directed(FLAGS_directed);
  graph_spec.set_rebalance(FLAGS_rebalance, FLAGS_rebalance_vertex_factor);
  if (FLAGS_deserialize) {
    graph_spec.set_deserialize(true, FLAGS_serialization_prefix);
  } else if (FLAGS_serialize) {
    graph_spec.set_serialize(true, FLAGS_serialization_prefix);
  }
  std::shared_ptr<FRAG_T> fragment;
  if (datasource == "local") {
    fragment = grape::LoadGraph<FRAG_T>(efile, vfile, comm_spec, graph_spec);
  } else {
    LOG(FATAL) << "Invalid datasource: " << datasource;
  }

  auto app = std::make_shared<APP_T>();
  auto worker = APP_T::CreateWorker(app, fragment);
  worker->Init(comm_spec, spec);
  worker->Query(std::forward<Args>(args)...);

  if (datasource == "local") {
    std::ofstream ostream;
    std::string output_path =
        grape::GetResultFilename(out_prefix, fragment->fid());
    ostream.open(output_path);
    worker->Output(ostream);
    ostream.close();
  }
  worker->Finalize();

  VLOG(1) << "Worker-" << comm_spec.worker_id() << " finished";
}

template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T, typename PARTITIONER_T>
void Run() {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  std::string efile = FLAGS_efile;
  std::string vfile = FLAGS_vfile;
  std::string out_prefix = FLAGS_out_prefix;
  auto spec = grape::DefaultParallelEngineSpec();
  if (FLAGS_app_concurrency != -1) {
    spec.thread_num = FLAGS_app_concurrency;
  } else {
    spec = MultiProcessSpec(comm_spec, false);
  }
  int fnum = comm_spec.fnum();
  std::string name = FLAGS_application;
  using VertexMapType = grape::GlobalVertexMap<OID_T, VID_T, PARTITIONER_T>;
  if (name == "sssp") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, double, grape::LoadStrategy::kOnlyOut, VertexMapType>;
    using AppType = grape::SSSP<GraphType>;
    CreateAndQuery<GraphType, AppType, OID_T>(comm_spec, efile, vfile,
                                              out_prefix, FLAGS_datasource,
                                              fnum, spec, FLAGS_sssp_source);
  } else if (name == "sssp_has_path") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, double, grape::LoadStrategy::kOnlyOut, VertexMapType>;
    using AppType = gs::SSSPHasPath<GraphType>;
    CreateAndQuery<GraphType, AppType, OID_T>(
        comm_spec, efile, vfile, out_prefix, FLAGS_datasource, fnum, spec,
        FLAGS_sssp_source, FLAGS_sssp_target);
  } else if (name == "sssp_average_length") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, double, grape::LoadStrategy::kOnlyOut, VertexMapType>;
    using AppType = SSSPAverageLength<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "sssp_path") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, double, grape::LoadStrategy::kOnlyOut, VertexMapType>;
    using AppType = SSSPPath<GraphType>;
    CreateAndQuery<GraphType, AppType, OID_T>(comm_spec, efile, vfile,
                                              out_prefix, FLAGS_datasource,
                                              fnum, spec, FLAGS_sssp_source);
  } else if (name == "cdlp_auto") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::CDLPAuto<GraphType>;
    CreateAndQuery<GraphType, AppType, int>(comm_spec, efile, vfile, out_prefix,
                                            FLAGS_datasource, fnum, spec, 10);
  } else if (name == "cdlp") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::CDLP<GraphType>;
    CreateAndQuery<GraphType, AppType, int>(comm_spec, efile, vfile, out_prefix,
                                            FLAGS_datasource, fnum, spec, 10);
  } else if (name == "sssp_auto") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, double,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::SSSPAuto<GraphType>;
    CreateAndQuery<GraphType, AppType, OID_T>(comm_spec, efile, vfile,
                                              out_prefix, FLAGS_datasource,
                                              fnum, spec, FLAGS_sssp_source);
  } else if (name == "wcc_auto") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::WCCAuto<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "wcc") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::WCC<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "lcc_auto") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::LCCAuto<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "lcc") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::LCC<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "bfs_auto") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::BFSAuto<GraphType>;
    CreateAndQuery<GraphType, AppType, OID_T>(comm_spec, efile, vfile,
                                              out_prefix, FLAGS_datasource,
                                              fnum, spec, FLAGS_bfs_source);
  } else if (name == "bfs_parallel") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::BFS<GraphType>;
    CreateAndQuery<GraphType, AppType, OID_T>(comm_spec, efile, vfile,
                                              out_prefix, FLAGS_datasource,
                                              fnum, spec, FLAGS_bfs_source);
  } else if (name == "pagerank_auto") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::PageRankAuto<GraphType>;
    CreateAndQuery<GraphType, AppType, double, int>(
        comm_spec, efile, vfile, out_prefix, FLAGS_datasource, fnum, spec, 0.85,
        10);
  } else if (name == "pagerank") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::PageRank<GraphType>;
    CreateAndQuery<GraphType, AppType, double, int>(
        comm_spec, efile, vfile, out_prefix, FLAGS_datasource, fnum, spec, 0.85,
        10);
  } else if (name == "kcore") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = KCore<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec,
                                       FLAGS_kcore_k);
  } else if (name == "kshell") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = KShell<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec,
                                       FLAGS_kshell_k);
  } else if (name == "hits") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = HITS<GraphType>;
    CreateAndQuery<GraphType, AppType>(
        comm_spec, efile, vfile, out_prefix, FLAGS_datasource, fnum, spec,
        FLAGS_hits_tolerance, FLAGS_hits_max_round, FLAGS_hits_normalized);
    // TODO(@weibin): uncomment once immutable_edgecut_fragment support
    // directed()
    /*
    } else if (name == "katz") {
      using GraphType =
          grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                          grape::LoadStrategy::kBothOutIn>;
      using AppType = KatzCentrality<GraphType>;
      CreateAndQuery<GraphType, AppType>(
          comm_spec, efile, vfile, out_prefix, FLAGS_datasource, fnum, spec,
          FLAGS_katz_centrality_alpha, FLAGS_katz_centrality_beta,
          FLAGS_katz_centrality_tolerance, FLAGS_katz_centrality_max_round,
          FLAGS_katz_centrality_normalized);
    } else if (name == "eigenvector") {
      using GraphType =
          grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                          grape::LoadStrategy::kBothOutIn>;
      using AppType = EigenvectorCentrality<GraphType>;
      CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                         FLAGS_datasource, fnum, spec,
                                         FLAGS_eigenvector_centrality_tolerance,
                                         FLAGS_eigenvector_centrality_max_round);
    */
  } else if (name == "bfs") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = BFSGeneric<GraphType>;
    CreateAndQuery<GraphType, AppType>(
        comm_spec, efile, vfile, out_prefix, FLAGS_datasource, fnum, spec,
        FLAGS_bfs_source, FLAGS_bfs_depth_limit, FLAGS_bfs_output_format);
  } else if (name == "degree_centrality") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = DegreeCentrality<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec,
                                       FLAGS_degree_centrality_type);
  } else if (name == "triangles") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kOnlyOut, VertexMapType>;
    using AppType = Triangles<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "clustering") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = Clustering<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "avg_clustering") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = AvgClustering<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "transitivity") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = Transitivity<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec);
  } else if (name == "dfs") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = DFS<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec,
                                       FLAGS_dfs_source, FLAGS_dfs_format);
  } else if (name == "bfs_original") {
    using GraphType =
        grape::ImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                        grape::LoadStrategy::kBothOutIn, VertexMapType>;
    using AppType = grape::BFS<GraphType>;
    CreateAndQuery<GraphType, AppType>(comm_spec, efile, vfile, out_prefix,
                                       FLAGS_datasource, fnum, spec,
                                       FLAGS_bfs_source);
  } else {
    LOG(FATAL) << "No available application named [" << name << "].";
  }

#ifdef GRANULA
  granula::operation offloadGraph("grape", "Id.Unique", "OffloadGraph",
                                  "Id.Unique");
#endif

#ifdef GRANULA
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    std::cout << offloadGraph.getOperationInfo("StartTime",
                                               offloadGraph.getEpoch())
              << std::endl;

    std::cout << grapeJob.getOperationInfo("EndTime", grapeJob.getEpoch())
              << std::endl;
  }

  granula::stopMonitorProcess(getpid());
#endif
}

extern "C" void RunApp() {
  if (FLAGS_segmented_partition) {
    Run<int64_t, uint32_t, grape::EmptyType, grape::EmptyType, grape::SegmentedPartitioner<int64_t>>();
  } else {
    FLAGS_rebalance = false;
    Run<int64_t, uint32_t, grape::EmptyType, grape::EmptyType, grape::HashPartitioner<int64_t>>();
  }
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_TEST_RUN_APP_H_
