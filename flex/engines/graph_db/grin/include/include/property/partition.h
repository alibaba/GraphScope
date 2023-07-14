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
 @file property/partition.h
 @brief Define the partition related APIs under property graph
 This file will be deprecated in the future.
 Partition schema related APIs will be moved to partition/property.h
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_PROPERTY_PARTITION_H_
#define GRIN_INCLUDE_PROPERTY_PARTITION_H_


#if defined(GRIN_ENABLE_GRAPH_PARTITION) && \
    !defined(GRIN_ASSUME_ALL_REPLICATE_PARTITION) && \
    !defined(GRIN_ASSUME_EDGE_CUT_PARTITION) && \
    !defined(GRIN_ASSUME_VERTEX_CUT_PARTITION)
// vertex partition
GRIN_VERTEX_TYPE_LIST grin_get_all_replicated_partition_vertex_types(GRIN_GRAPH);
GRIN_VERTEX_TYPE_LIST grin_get_disjoint_partition_vertex_types(GRIN_GRAPH);
GRIN_VERTEX_TYPE_LIST grin_get_follow_edge_partition_vertex_types(GRIN_GRAPH);

// edge partition
GRIN_VEV_TYPE_LIST grin_get_all_replicated_partition_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_disjoint_partition_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_follow_src_partition_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_follow_dst_partition_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_follow_both_partition_vev_types(GRIN_GRAPH);
#endif


// vertex property partition
#if defined(GRIN_ENABLE_GRAPH_PARTITION) && \
    defined(GRIN_WITH_VERTEX_PROPERTY) && \
    !defined(GRIN_ASSUME_MASTER_ONLY_PARTITION_FOR_VERTEX_PROPERTY) && \
    !defined(GRIN_ASSUME_REPLICATE_MASTER_MIRROR_PARTITION_FOR_VERTEX_PROPERTY) && \
    !defined(GRIN_ASSUME_SPLIT_MASTER_MIRROR_PARTITION_FOR_VERTEX_PROPERTY)
GRIN_VERTEX_TYPE_LIST grin_get_master_only_partition_vertex_types(GRIN_GRAPH);
GRIN_VERTEX_TYPE_LIST grin_get_replicate_master_mirror_partition_vertex_types(GRIN_GRAPH);
GRIN_VERTEX_TYPE_LIST grin_get_split_master_mirror_partition_vertex_types(GRIN_GRAPH);
#endif

// edge property partition
#if defined(GRIN_ENABLE_GRAPH_PARTITION) && \
    defined(GRIN_WITH_EDGE_PROPERTY) && \
    !defined(GRIN_ASSUME_MASTER_ONLY_PARTITION_FOR_EDGE_PROPERTY) && \
    !defined(GRIN_ASSUME_REPLICATE_MASTER_MIRROR_PARTITION_FOR_EDGE_PROPERTY) && \
    !defined(GRIN_ASSUME_SPLIT_MASTER_MIRROR_PARTITION_FOR_EDGE_PROPERTY)
GRIN_VEV_TYPE_LIST grin_get_master_only_partition_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_replicate_master_mirror_partition_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_split_master_mirror_partition_vev_types(GRIN_GRAPH);
#endif

// vev relation
#ifdef GRIN_TRAIT_SPECIFIC_VEV_RELATION
GRIN_VEV_TYPE_LIST grin_get_one_to_one_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_one_to_many_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_many_to_one_vev_types(GRIN_GRAPH);
GRIN_VEV_TYPE_LIST grin_get_many_to_many_vev_types(GRIN_GRAPH);
#endif

#endif // GRIN_INCLUDE_PROPERTY_PARTITION_H_

#ifdef __cplusplus
}
#endif