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
 @file pk.h
 @brief Define the primary key indexing related APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_INDEX_PK_H_
#define GRIN_INCLUDE_INDEX_PK_H_

#if defined(GRIN_ENABLE_VERTEX_PK_INDEX) && defined(GRIN_ENABLE_VERTEX_PRIMARY_KEYS)
/**
 * @brief Get the vertex by primary keys row.
 * The values in the row must be in the same order as the primary keys 
 * properties, which can be obtained by ``grin_get_primary_keys_by_vertex_type``.
 * @param GRIN_GRAPH The graph.
 * @param GRIN_VERTEX_TYPE The vertex type.
 * @param GRIN_ROW The values row of primary keys properties.
 * @return The vertex.
*/
GRIN_VERTEX grin_get_vertex_by_primary_keys_row(GRIN_GRAPH, GRIN_VERTEX_TYPE, GRIN_ROW);
#endif

#if defined(GRIN_ENABLE_EDGE_PK_INDEX) && defined(GRIN_ENABLE_EDGE_PRIMARY_KEYS)
GRIN_EDGE grin_get_edge_by_primary_keys_row(GRIN_GRAPH, GRIN_EDGE_TYPE, GRIN_ROW);
#endif

#endif // GRIN_INCLUDE_INDEX_PK_H_

#ifdef __cplusplus
}
#endif