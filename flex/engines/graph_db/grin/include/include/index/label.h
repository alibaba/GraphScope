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
 @file label.h
 @brief Define the label related APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_INDEX_LABEL_H_
#define GRIN_INCLUDE_INDEX_LABEL_H_


#if defined(GRIN_WITH_VERTEX_LABEL) || defined(GRIN_WITH_EDGE_LABEL)
GRIN_LABEL grin_get_label_by_name(GRIN_GRAPH, const char*);

void grin_destroy_label(GRIN_GRAPH, GRIN_LABEL);

const char* grin_get_label_name(GRIN_GRAPH, GRIN_LABEL);

void grin_destroy_label_list(GRIN_GRAPH, GRIN_LABEL_LIST);

size_t grin_get_label_list_size(GRIN_GRAPH, GRIN_LABEL_LIST);

GRIN_LABEL grin_get_label_from_list(GRIN_GRAPH, GRIN_LABEL_LIST, size_t);
#endif

#ifdef GRIN_WITH_VERTEX_LABEL
/**
 * @brief assign a label to a vertex
 * @param GRIN_GRAPH the graph
 * @param GRIN_LABEL the label
 * @param GRIN_VERTEX the vertex
 * @return whether succeed
*/
bool grin_assign_label_to_vertex(GRIN_GRAPH, GRIN_LABEL, GRIN_VERTEX);

/**
 * @brief get the label list of a vertex
 * @param GRIN_GRAPH the graph
 * @param GRIN_VERTEX the vertex
*/
GRIN_LABEL_LIST grin_get_vertex_label_list(GRIN_GRAPH, GRIN_VERTEX);

/**
 * @brief get the vertex list by label
 * @param GRIN_GRAPH the graph
 * @param GRIN_LABEL the label
*/
GRIN_VERTEX_LIST grin_get_vertex_list_by_label(GRIN_GRAPH, GRIN_LABEL);

/**
 * @brief filtering an existing vertex list by label
 * @param GRIN_VERTEX_LIST the existing vertex list
 * @param GRIN_LABEL the label
*/
GRIN_VERTEX_LIST grin_select_label_for_vertex_list(GRIN_GRAPH, GRIN_LABEL, GRIN_VERTEX_LIST);
#endif

#ifdef GRIN_WITH_EDGE_LABEL
/**
 * @brief assign a label to a edge
 * @param GRIN_GRAPH the graph
 * @param GRIN_LABEL the label
 * @param GRIN_EDGE the edge
 * @return whether succeed
*/
bool grin_assign_label_to_edge(GRIN_GRAPH, GRIN_LABEL, GRIN_EDGE);

/**
 * @brief get the label list of a edge
 * @param GRIN_GRAPH the graph
 * @param GRIN_EDGE the edge
*/
GRIN_LABEL_LIST grin_get_edge_label_list(GRIN_GRAPH, GRIN_EDGE);

/**
 * @brief get the edge list by label
 * @param GRIN_GRAPH the graph
 * @param GRIN_LABEL the label
*/
GRIN_EDGE_LIST grin_get_edge_list_by_label(GRIN_GRAPH, GRIN_LABEL);

/**
 * @brief filtering an existing edge list by label
 * @param GRIN_EDGE_LIST the existing edge list
 * @param GRIN_LABEL the label
*/
GRIN_EDGE_LIST grin_select_label_for_edge_list(GRIN_GRAPH, GRIN_LABEL, GRIN_EDGE_LIST);
#endif

#endif // GRIN_INCLUDE_INDEX_LABEL_H_

#ifdef __cplusplus
}
#endif