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

#ifndef ANALYTICAL_ENGINE_CORE_FLAGS_H_
#define ANALYTICAL_ENGINE_CORE_FLAGS_H_

#include <gflags/gflags_declare.h>

DECLARE_string(host);
DECLARE_int32(port);

DECLARE_string(dag_file);

// vineyard
DECLARE_string(vineyard_socket);
DECLARE_string(vineyard_shared_mem);
DECLARE_string(etcd_endpoint);

DECLARE_bool(batch_mode);

// applications
DECLARE_string(application);
DECLARE_bool(directed);
DECLARE_string(efile);
DECLARE_string(vfile);
DECLARE_string(out_prefix);

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

DECLARE_int64(dfs_source);
DECLARE_string(dfs_format);

DECLARE_int32(vr_num_of_nodes);

DECLARE_string(sampling_path_pattern);
DECLARE_bool(run_projected);

DECLARE_double(pagerank_delta);
DECLARE_int32(max_round);
#endif  // ANALYTICAL_ENGINE_CORE_FLAGS_H_
