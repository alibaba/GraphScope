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
 @file row.h
 @brief Define the row related APIs
*/

#ifdef __cplusplus
extern "C" {
#endif

#ifndef GRIN_INCLUDE_PROPERTY_ROW_H_
#define GRIN_INCLUDE_PROPERTY_ROW_H_


#ifdef GRIN_ENABLE_ROW
void grin_destroy_row(GRIN_GRAPH, GRIN_ROW);

int grin_get_int32_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

unsigned int grin_get_uint32_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

long long int grin_get_int64_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

unsigned long long int grin_get_uint64_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

float grin_get_float_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

double grin_get_double_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

const char* grin_get_string_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

int grin_get_date32_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

int grin_get_time32_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

long long int grin_get_timestamp64_from_row(GRIN_GRAPH, GRIN_ROW, size_t);

/**
 * @brief Create a row.
 * Row works as carrier of property values in GRIN.
 * It is a pure value array, and users can only get the value by the array index.
 * That means users should understand the property that each value is 
 * representing when using the row.
 * Currently rows are used in two scenarios:
 * 1. Users can create a row of values for primary keys properties,
 * and then query the vertex/edge using the row if pk indexing is enabled.
 * 2. Users can get the row of values for the entire property list of 
 * a vertex/edge in one API ``grin_get_vertex_row`` or ``grin_get_edge_row``.
 * However this API is not recommended if the user only wants to get the
 * properties values, in which case, the user can get property values
 * one-by-one using the APIs like ``grin_get_vertex_property_value_of_int32``.
*/
GRIN_ROW grin_create_row(GRIN_GRAPH);

bool grin_insert_int32_to_row(GRIN_GRAPH, GRIN_ROW, int);

bool grin_insert_uint32_to_row(GRIN_GRAPH, GRIN_ROW, unsigned int);

bool grin_insert_int64_to_row(GRIN_GRAPH, GRIN_ROW, long long int);

bool grin_insert_uint64_to_row(GRIN_GRAPH, GRIN_ROW, unsigned long long int);

bool grin_insert_float_to_row(GRIN_GRAPH, GRIN_ROW, float);

bool grin_insert_double_to_row(GRIN_GRAPH, GRIN_ROW, double);

bool grin_insert_string_to_row(GRIN_GRAPH, GRIN_ROW, const char*);

bool grin_insert_date32_to_row(GRIN_GRAPH, GRIN_ROW, int);

bool grin_insert_time32_to_row(GRIN_GRAPH, GRIN_ROW, int);

bool grin_insert_timestamp64_to_row(GRIN_GRAPH, GRIN_ROW, long long int);
#endif

#if defined(GRIN_ENABLE_ROW) && defined(GRIN_TRAIT_CONST_VALUE_PTR)
const void* grin_get_value_from_row(GRIN_GRAPH, GRIN_ROW, GRIN_DATATYPE, size_t);
#endif


#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_ENABLE_ROW)
/**
 * @brief Get row of values for the entire property list of a vertex.
 * Later users can get property values from the row using APIs like
 * ``grin_get_int32_from_row``.
 * However this two-step value getting is not recommended if the user 
 * only wants to get the value of one property, in which case, the user
 * should use APIs like ``grin_get_vertex_property_value_of_int32``.
 * @param GRIN_GRAPH The graph
 * @param GRIN_VERTEX The vertex
 */
GRIN_ROW grin_get_vertex_row(GRIN_GRAPH, GRIN_VERTEX);
#endif


#if defined(GRIN_WITH_EDGE_PROPERTY) && defined(GRIN_ENABLE_ROW)
GRIN_ROW grin_get_edge_row(GRIN_GRAPH, GRIN_EDGE);
#endif

#endif  // GRIN_INCLUDE_PROPERTY_ROW_H_

#ifdef __cplusplus
}
#endif