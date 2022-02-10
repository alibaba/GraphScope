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

#include "test/run_app.h"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

DEFINE_string(application, "", "application name");
DEFINE_string(efile, "", "edge file");
DEFINE_string(vfile, "", "vertex file");
DEFINE_string(out_prefix, "", "output directory of results");
DEFINE_string(datasource, "local",
              "datasource type, available options: local, odps, oss");
DEFINE_string(jobid, "", "jobid, only used in LDBC graphanalytics.");
DEFINE_bool(directed, false, "input graph is directed or not.");

/* flags related to specific applications. */
DEFINE_int64(bfs_source, 0, "source vertex of bfs.");
DEFINE_string(degree_centrality_type, "both",
              "the type of degree centrality, available options: in/out/bot");

DEFINE_double(eigenvector_centrality_tolerance, 1e-6, "Error tolerance.");
DEFINE_int32(eigenvector_centrality_max_round, 100,
             "Maximum number of iterations.");

DEFINE_double(hits_tolerance, 0.001, "Error tolerance.");
DEFINE_int32(hits_max_round, 100, "Maximum number of iterations.");
DEFINE_bool(hits_normalized, true,
            "Normalize results by the sum of all of the values.");

DEFINE_int32(kcore_k, 3, "The order of the core");

DEFINE_int32(kshell_k, 3, "The order of the shell");

DEFINE_double(katz_centrality_alpha, 0.1, "Attenuation factor");
DEFINE_double(katz_centrality_beta, 1.0,
              "Weight attributed to the immediate neighborhood.");
DEFINE_double(katz_centrality_tolerance, 1e-06, "Error tolerance.");
DEFINE_int32(katz_centrality_max_round, 100, "Maximum number of iterations.");
DEFINE_bool(katz_centrality_normalized, true,
            "Normalize results by the sum of all of the values.");

DEFINE_int64(sssp_source, 0, "Source vertex of sssp.");
DEFINE_int64(sssp_target, 1, "Target vertex of sssp.");
DEFINE_bool(
    sssp_weight, true,
    "If true, use edge attribute as weight. Otherwise, all use weight 1.");

DEFINE_int32(bfs_depth_limit, 10, "Specify the maximum search depth.");
DEFINE_string(bfs_output_format, "edges",
              "Output format[edges/predecessors/successors].");

DEFINE_bool(segmented_partition, true,
            "whether to use segmented partitioning.");
DEFINE_bool(rebalance, true, "whether to rebalance graph after loading.");
DEFINE_int32(rebalance_vertex_factor, 0, "vertex factor of rebalancing.");

DEFINE_bool(serialize, false, "whether to serialize loaded graph.");
DEFINE_bool(deserialize, false, "whether to deserialize graph while loading.");
DEFINE_string(serialization_prefix, "",
              "where to load/store the serialization files");

DEFINE_int32(app_concurrency, -1, "concurrency of application");

DEFINE_int64(dfs_source, 0, "source vertex of dfs.");
DEFINE_string(dfs_format, "edges", "output format of dfs.");

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./run_app [grape_opts]");
  if (argc == 1) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "analytical_apps");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging("analytical_apps");
  google::InstallFailureSignalHandler();

  gs::Init();

  std::string name = FLAGS_application;
  if (name.find("sssp") != std::string::npos ||
      name.find("eigenvector") != std::string::npos) {
    if (FLAGS_segmented_partition) {
      gs::Run<int64_t, uint32_t, grape::EmptyType, double, grape::SegmentedPartitioner<int64_t>>();
    } else {
      gs::Run<int64_t, uint32_t, grape::EmptyType, double, grape::HashPartitioner<int64_t>>();
    }
  } else {
    if (FLAGS_segmented_partition) {
      gs::Run<int64_t, uint32_t, grape::EmptyType, grape::EmptyType, grape::SegmentedPartitioner<int64_t>>();
    } else {
      gs::Run<int64_t, uint32_t, grape::EmptyType, grape::EmptyType, grape::HashPartitioner<int64_t>>();
    }
  }

  gs::Finalize();

  google::ShutdownGoogleLogging();
}
