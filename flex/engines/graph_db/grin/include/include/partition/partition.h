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

/**
 @file partition.h
 @brief Define the partition related APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_PARTITION_PARTITION_H_
#define GRIN_INCLUDE_PARTITION_PARTITION_H_


#ifdef GRIN_ENABLE_GRAPH_PARTITION
/**
 * @brief Get a partitioned graph from a storage.
 * @param uri The URI of the graph.
 * Current URI for supported storage includes:
 * 1. gart://{etcd_endpoint}?prefix={etcd_prefix}&version={version}
 * 2. graphar://{yaml_path}?partition_num={partition_num}&strategy={strategy}
 * 3. v6d://{object_id}?ipc_socket={ipc_socket} where ipc_socket is optional.
 * @return A partitioned graph handle.
*/
GRIN_PARTITIONED_GRAPH grin_get_partitioned_graph_from_storage(const char* uri);

void grin_destroy_partitioned_graph(GRIN_PARTITIONED_GRAPH);

size_t grin_get_total_partitions_number(GRIN_PARTITIONED_GRAPH);

/**
 * @brief Get the local partition list of the partitioned graph.
 * For example, a graph may be partitioned into 6 partitions and located in
 * 2 machines, then each machine may contain a local partition list of size 3.
 * @param GRIN_PARTITIONED_GRAPH The partitioned graph.
 * @return A partition list of local partitions.
*/
GRIN_PARTITION_LIST grin_get_local_partition_list(GRIN_PARTITIONED_GRAPH);

void grin_destroy_partition_list(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION_LIST);

GRIN_PARTITION_LIST grin_create_partition_list(GRIN_PARTITIONED_GRAPH);

bool grin_insert_partition_to_list(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION_LIST, GRIN_PARTITION);

size_t grin_get_partition_list_size(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION_LIST);

GRIN_PARTITION grin_get_partition_from_list(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION_LIST, size_t);

bool grin_equal_partition(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION, GRIN_PARTITION);

void grin_destroy_partition(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION);

const void* grin_get_partition_info(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION);

/**
 * @brief Get a local graph of the partitioned graph.
 * @param GRIN_PARTITIONED_GRAPH The partitioned graph.
 * @param GRIN_PARTITION The partition of the graph.
 * @return A local graph.
*/
GRIN_GRAPH grin_get_local_graph_by_partition(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION);
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_PARTITION
GRIN_PARTITION grin_get_partition_by_id(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION_ID);

GRIN_PARTITION_ID grin_get_partition_id(GRIN_PARTITIONED_GRAPH, GRIN_PARTITION);
#endif

#endif  // GRIN_INCLUDE_PARTITION_PARTITION_H_

#ifdef __cplusplus
}
#endif
