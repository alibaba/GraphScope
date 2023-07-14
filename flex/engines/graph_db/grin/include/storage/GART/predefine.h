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
 * @file predefine.h
 * @brief This template file consists of four parts:
 * 1. The predefined enumerate types of GRIN, which should NOT be modified.
 * 2. The supported macros which should be specified by storage implementors
 * based on storage features.
 * 3. The typedefs of the enabled handles. This should be specified by storage.
 * 4. The corresponding null values of the enabled handles. This should be
 * specified by storage.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>

/* 1. Predefined enumerate types of GRIN */
/// Enumerates the directions of edges with respect to a certain vertex
typedef enum {
  IN = 0,     ///< incoming
  OUT = 1,    ///< outgoing
  BOTH = 2,   ///< incoming & outgoing
} GRIN_DIRECTION;

/// Enumerates the datatype supported in the storage
typedef enum {
  Undefined = 0,      ///< other unknown types
  Int32 = 1,          ///< int
  UInt32 = 2,         ///< unsigned int
  Int64 = 3,          ///< long int
  UInt64 = 4,         ///< unsigned long int
  Float = 5,          ///< float
  Double = 6,         ///< double
  String = 7,         ///< string
  Date32 = 8,         ///< date
  Time32 = 9,         ///< Time32
  Timestamp64 = 10,   ///< Timestamp
} GRIN_DATATYPE;

/// Enumerates the error codes of grin
typedef enum {
  NO_ERROR = 0,              ///< success
  UNKNOWN_ERROR = 1,         ///< unknown error
  INVALID_VALUE = 2,         ///< invalid value
  UNKNOWN_DATATYPE = 3,      ///< unknown datatype
} GRIN_ERROR_CODE;

/* Define supported macros based on storage features */
// Topology
#define GRIN_ASSUME_HAS_DIRECTED_GRAPH
#define GRIN_ASSUME_HAS_UNDIRECTED_GRAPH
#define GRIN_ASSUME_HAS_MULTI_EDGE_GRAPH
#define GRIN_ENABLE_VERTEX_LIST
#define GRIN_ENABLE_VERTEX_LIST_ITERATOR
#define GRIN_ENABLE_ADJACENT_LIST
#define GRIN_ENABLE_ADJACENT_LIST_ITERATOR
// Partition
#define GRIN_ENABLE_GRAPH_PARTITION
#define GRIN_TRAIT_NATURAL_ID_FOR_PARTITION
#define GRIN_ENABLE_VERTEX_REF
#define GRIN_TRAIT_FAST_VERTEX_REF
#define GRIN_ASSUME_EDGE_CUT_PARTITION
#define GRIN_TRAIT_SELECT_MASTER_FOR_VERTEX_LIST
// Property
#define GRIN_ENABLE_ROW
#define GRIN_WITH_VERTEX_PROPERTY
#define GRIN_WITH_VERTEX_PROPERTY_NAME
#define GRIN_WITH_VERTEX_TYPE_NAME
#define GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE
#define GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY
#define GRIN_WITH_EDGE_PROPERTY
#define GRIN_WITH_EDGE_PROPERTY_NAME
#define GRIN_WITH_EDGE_TYPE_NAME
#define GRIN_TRAIT_NATURAL_ID_FOR_EDGE_TYPE
#define GRIN_TRAIT_NATURAL_ID_FOR_EDGE_PROPERTY
// Index
#define GRIN_ENABLE_VERTEX_INTERNAL_ID_INDEX

/* Define the handles using typedef */
typedef void* GRIN_GRAPH;
typedef unsigned long long int GRIN_VERTEX;                 

#ifdef GRIN_WITH_VERTEX_DATA
typedef void* GRIN_VERTEX_DATA;                 
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST
typedef void* GRIN_VERTEX_LIST;                 
#endif

#ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
typedef void* GRIN_VERTEX_LIST_ITERATOR;         
#endif

#ifdef GRIN_ENABLE_ADJACENT_LIST_ITERATOR
typedef void* GRIN_ADJACENT_LIST_ITERATOR;       
#endif

#ifdef GRIN_WITH_EDGE_DATA
typedef void* GRIN_EDGE_DATA;                   
#endif

#ifdef GRIN_ENABLE_EDGE_LIST
typedef void* GRIN_EDGE_LIST;                   
#endif

#ifdef GRIN_ENABLE_EDGE_LIST_ITERATOR
typedef void* GRIN_EDGE_LIST_ITERATOR;           
#endif

#ifdef GRIN_ENABLE_GRAPH_PARTITION
typedef void* GRIN_PARTITIONED_GRAPH;
typedef unsigned GRIN_PARTITION;
typedef void* GRIN_PARTITION_LIST;
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_PARTITION
typedef unsigned GRIN_PARTITION_ID;
#endif

#ifdef GRIN_ENABLE_VERTEX_REF
typedef long long int GRIN_VERTEX_REF;
#endif

#ifdef GRIN_ENABLE_EDGE_REF
typedef void* GRIN_EDGE_REF;
#endif

#ifdef GRIN_WITH_VERTEX_PROPERTY
typedef unsigned GRIN_VERTEX_TYPE;
typedef void* GRIN_VERTEX_TYPE_LIST;
typedef unsigned long long int GRIN_VERTEX_PROPERTY;
typedef void* GRIN_VERTEX_PROPERTY_LIST;
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE
typedef unsigned GRIN_VERTEX_TYPE_ID;
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY
typedef unsigned GRIN_VERTEX_PROPERTY_ID;
#endif

#ifdef GRIN_WITH_EDGE_PROPERTY
typedef unsigned GRIN_EDGE_TYPE;
typedef void* GRIN_EDGE_TYPE_LIST;
typedef void* GRIN_VEV_TYPE;
typedef void* GRIN_VEV_TYPE_LIST;
typedef unsigned long long int GRIN_EDGE_PROPERTY;
typedef void* GRIN_EDGE_PROPERTY_LIST;
#endif

typedef struct GRIN_EDGE {
  GRIN_VERTEX src;
  GRIN_VERTEX dst;
  GRIN_DIRECTION dir;
  GRIN_EDGE_TYPE etype;
  char* edata;
} GRIN_EDGE;     

#ifdef GRIN_ENABLE_ADJACENT_LIST
typedef struct GRIN_ADJACENT_LIST {
  GRIN_VERTEX v;
  GRIN_DIRECTION dir;
  GRIN_EDGE_TYPE etype;
} GRIN_ADJACENT_LIST;               
#endif


#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_TYPE
typedef unsigned GRIN_EDGE_TYPE_ID;
#endif

#ifdef GRIN_TRAIT_NATURAL_ID_FOR_EDGE_PROPERTY
typedef unsigned GRIN_EDGE_PROPERTY_ID;
#endif

#ifdef GRIN_ENABLE_ROW
typedef void* GRIN_ROW;
#endif

#if defined(GRIN_WITH_VERTEX_LABEL) || defined(GRIN_WITH_EDGE_LABEL)
typedef void* GRIN_LABEL;
typedef void* GRIN_LABEL_LIST;
#endif

/* Define invalid values for returns of handles */
#define GRIN_NULL_GRAPH NULL
#define GRIN_NULL_VERTEX (unsigned long long int)~0
#define GRIN_NULL_VERTEX_LIST NULL
#define GRIN_NULL_VERTEX_LIST_ITERATOR NULL
#define GRIN_NULL_ADJACENT_LIST_ITERATOR NULL
#define GRIN_NULL_PARTITIONED_GRAPH NULL
#define GRIN_NULL_PARTITION (unsigned)~0
#define GRIN_NULL_PARTITION_LIST NULL
#define GRIN_NULL_PARTITION_ID (unsigned)~0
#define GRIN_NULL_VERTEX_REF -1
#define GRIN_NULL_VERTEX_TYPE (unsigned)~0
#define GRIN_NULL_VERTEX_TYPE_LIST NULL
#define GRIN_NULL_VERTEX_PROPERTY (unsigned long long int)~0
#define GRIN_NULL_VERTEX_PROPERTY_LIST NULL
#define GRIN_NULL_VERTEX_TYPE_ID (unsigned)~0
#define GRIN_NULL_VERTEX_PROPERTY_ID (unsigned)~0
#define GRIN_NULL_EDGE_TYPE (unsigned)~0
#define GRIN_NULL_EDGE_TYPE_LIST NULL
#define GRIN_NULL_VEV_TYPE NULL
#define GRIN_NULL_VEV_TYPE_LIST NULL
#define GRIN_NULL_EDGE_PROPERTY (unsigned long long int)~0
#define GRIN_NULL_EDGE_PROPERTY_LIST NULL
#define GRIN_NULL_EDGE_TYPE_ID (unsigned)~0
#define GRIN_NULL_EDGE_PROPERTY_ID (unsigned)~0
#define GRIN_NULL_ROW NULL
#define GRIN_NULL_SIZE (unsigned)~0
#define GRIN_NULL_NAME NULL
#define GRIN_NULL_EDGE GRIN_EDGE{GRIN_NULL_VERTEX, GRIN_NULL_VERTEX, BOTH, GRIN_NULL_EDGE_TYPE_ID, NULL}
#define GRIN_NULL_ADJACENT_LIST GRIN_ADJACENT_LIST{GRIN_NULL_VERTEX, BOTH, GRIN_NULL_EDGE_TYPE_ID}

#ifdef __cplusplus
}
#endif
