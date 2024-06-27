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

#include "core/flags.h"

/* flags related to the job. */

DEFINE_string(host, "localhost", "the host to listen by gRPC server");
DEFINE_int32(port, 60001, "the port to listen by gRPC server");

// for vineyard
DEFINE_string(vineyard_socket, "", "Unix domain socket path for vineyardd");
DEFINE_string(vineyard_shared_mem, "2048000000",
              "Init size of vineyard shared memory");
DEFINE_string(etcd_endpoint, "http://127.0.0.1:2379",
              "Etcd endpoint that will be used to launch vineyardd");

DEFINE_string(dag_file, "", "Engine reads serialized dag proto from dag_file.");

DEFINE_bool(batch_mode, false, "Whether to run in batch mode.");

DEFINE_string(application, "", "application name");
DEFINE_string(efile, "", "edge file");
DEFINE_string(vfile, "", "vertex file");
DEFINE_string(out_prefix, "", "output directory of results");
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

DEFINE_int64(dfs_source, 0, "source vertex of dfs.");
DEFINE_string(dfs_format, "edges", "output format of dfs.");

DEFINE_int32(vr_num_of_nodes, 0, "nodes number of voterank.");

DEFINE_string(sampling_path_pattern, "", "sampling path pattern");
DEFINE_bool(run_projected, false, "run projected");

DEFINE_double(pagerank_delta, 0.85, "damping factor of pagerank");
DEFINE_int32(max_round, 10, "maximum round");
