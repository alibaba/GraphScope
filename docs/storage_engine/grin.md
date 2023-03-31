# GRIN: Graph Retrieval INterface
GRIN is a proposed standard graph retrieval interface in GraphScope. Its goal is to provide a common way for graph computing engines to retrieve graph data stored in different storage engines within GraphScope, and to simplify the integration of these engines with each other.

GRIN is defined in C, making it portable to systems written in different programming languages, such as C++, Rust, and Java. It provides a set of common operations and data structure handlers that can be used to access graph data, regardless of the underlying storage engine.
* Traversal: navigating the graph structure to explore relationships between vertices
* Retrieval: retrieving the data and properties of vertices and edges
* Filter: filtering data structures based on partitioning or property conditions

The latest version of headers can be found in [GRIN](https://github.com/GraphScope/GRIN).
A Vineyard implementation for both computing and storage can be found in [Vineyard](https://github.com/v6d-io/v6d/tree/dev/grin/modules/graph/grin).

## Motivations
The motivations behind GRIN are driven by the need for a common standard for accessing graph data for the computing engines of GraphScope. There are several factors that have contributed to this need:
1. Complexity of integrating multiple graph computing engines and storage engines: GraphScope consists of numerous graph computing engines and storage engines, each with its own data model, query language, and API. This can make it challenging to integrate these engines with one another, as each engine requires a separate integration effort.
2. Need for interoperability and collaboration: To fully realize GraphScope's potential, greater interoperability and collaboration between different components are necessary. A common standard for accessing graph data could help accelerate GraphScope's development and make it more accessible to a broader range of users.
3. Growing demand for large-scale graph processing: As the volume of data being generated increases, there is a growing demand for graph processing systems that can efficiently analyze large-scale graph data. A common standard for accessing graph data could help accelerate the development of new features for engines in GraphScope.

By defining the GRIN interfaces in C, it can be easily integrated with various programming languages, including Java, Python, and Rust, using foreign function interfaces (FFIs) that enable these languages to call C functions. Moreover, GRIN becomes a language-agnostic standard that can be utilized across different programming environments without requiring each environment to have its own implementation of the standard. This greatly simplifies the development and maintenance of graph computing systems relying on GRIN, as it allows developers to use their preferred programming languages and tools while still being able to access and process graph data using a common standard.

## Unified Graph Retrieval
As previously mentioned, GRIN offers computing engines a consistent method for accessing graphs stored in various storage engines through a set of common operations for graph traversal, data retrieval, and conditional filtering. These operations are defined as C functions, and their return values and parameters are typically handlers for graph concepts, such as vertices and edges.

Here is an example showing how to handle a graph query using GRIN APIs. The data types with the prefix `GRIN_` are handlers, while the functions with the prefix `grin_` are the APIs in GRIN. Moreover, macros that start with `GRIN_` are provided by GRIN to reflect storage features. Each storage engine that implements GRIN APIs can set up these macros based on their own features, such as graph partition strategies and list retrieval styles. The storage provider for this example is Vineyard.

The example demonstrates how to synchronize property values of vertices associated with a specific edge type. The input parameters are `partitioned_graph`, the local `partition`, `edge_type_name` (e.g., likes), and `vertex_property_name` (e.g., features). The task is to find all the destination vertices of "boundary edges" with a type named "likes", and the vertices must have a property named "features". Here, a boundary edge is an edge whose source vertex is a master vertex, and the destination is a mirror vertex, given the context of the "edge-cut" partition strategy that the underlying storage uses. For each of these vertices, we send the value of the "features" property to its master partition.

```CPP
    void sync_property(GRIN_PARTITIONED_GRAPH partitioned_graph, GRIN_PARTITION partition, const char* edge_type_name, const char* vertex_property_name) {
        GRIN_GRAPH g = grin_get_local_graph_from_partition(partitioned_graph, partition);  // get local graph of partition

        GRIN_EDGE_TYPE etype = grin_get_edge_type_by_name(g, edge_type_name);  // get edge type from name
        GRIN_VERTEX_TYPE_LIST src_vtypes = grin_get_src_types_from_edge_type(g, etype);  // get related source vertex type list
        GRIN_VERTEX_TYPE_LIST dst_vtypes = grin_get_dst_types_from_edge_type(g, etype);  // get related destination vertex type list

        size_t src_vtypes_num = grin_get_vertex_type_list_size(g, src_vtypes);
        size_t dst_vtypes_num = grin_get_vertex_type_list_size(g, dst_vtypes);
        assert(src_vtypes_num == dst_vtypes_num);  // the src & dst vertex type lists must be aligned

        for (size_t i = 0; i < src_vtypes_num; ++i) {  // iterate all pairs of src & dst vertex type
            GRIN_VERTEX_TYPE src_vtype = grin_get_vertex_type_from_list(g, src_vtypes, i);  // get src type
            GRIN_VERTEX_TYPE dst_vtype = grin_get_vertex_type_from_list(g, dst_vtypes, i);  // get dst type

            GRIN_VERTEX_PROPERTY dst_vp = grin_get_vertex_property_by_name(g, dst_vtype, vertex_property_name);  // get the property called "features" under dst type
            if (dst_vp == GRIN_NULL_VERTEX_PROPERTY) continue;  // select out the pairs whose dst type does NOT have such a property called "features"
            
            GRIN_VERTEX_PROPERTY_TABLE dst_vpt = grin_get_vertex_property_table_by_type(g, dst_vtype);  // prepare property table of dst vertex type for later use
            GRIN_DATATYPE dst_vp_dt = grin_get_vertex_property_data_type(g, dst_vp); // prepare property type for later use

            GRIN_VERTEX_LIST __src_vl = grin_get_vertex_list(g);  // get the vertex list
            GRIN_VERTEX_LIST _src_vl = grin_select_type_for_vertex_list(g, src_vtype, __src_vl);  // select the vertex of source type
            GRIN_VERTEX_LIST src_vl = grin_select_master_for_vertex_list(g, _src_vl);  // select master vertices under source type
            
            size_t src_vl_num = grin_get_vertex_list_size(g, src_vl);
            for (size_t j = 0; j < src_vl_num; ++j) { // iterate the src vertex
                GRIN_VERTEX v = grin_get_vertex_from_list(g, src_vl, j);

            #ifdef GRIN_TRAIT_SELECT_EDGE_TYPE_FOR_ADJACENT_LIST
                GRIN_ADJACENT_LIST _adj_list = grin_get_adjacent_list(g, GRIN_DIRECTION::OUT, v);  // get the outgoing adjacent list of v
                GRIN_ADJACENT_LIST adj_list = grin_select_edge_type_for_adjacent_list(g, etype, _adj_list);  // select edges under etype
            #else
                GRIN_ADJACENT_LIST adj_lsit = grin_get_adjacent_list(g, GRIN_DIRECTION::OUT, v);  // get the outgoing adjacent list of v
            #endif

                size_t al_sz = grin_get_adjacent_list_size(g, adj_list);
                for (size_t k = 0; k < al_sz; ++k) {
            #ifndef GRIN_TRAIT_SELECT_EDGE_TYPE_FOR_ADJACENT_LIST
                    GRIN_EDGE edge = grin_get_edge_from_adjacent_list(g, adj_list, k);
                    GRIN_EDGE_TYPE edge_type = grin_get_edge_type(g, edge);
                    if (!grin_equal_edge_type(g, edge_type, etype)) continue;
            #endif
                    GRIN_VERTEX u = grin_get_neighbor_from_adjacent_list(g, adj_list, k);  // get the dst vertex u
                    const void* value = grin_get_value_from_vertex_property_table(g, dst_vpt, u, dst_vp);  // get the property value of "features" of u

                    GRIN_VERTEX_REF uref = grin_get_vertex_ref_for_vertex(g, u);  // get the reference of u that can be recoginized by other partitions
                    GRIN_PARTITION u_master_partition = grin_get_master_partition_from_vertex_ref(g, uref);  // get the master partition for u

                    send_value(u_master_partition, uref, dst_vp_dt, value);  // the value must be casted to the correct type based on dst_vp_dt before sending
                }
            }
        }
    }
    
    void run(vineyard::Client& client, const grape::CommSpec& comm_spec,
             vineyard::ObjectID fragment_group_id) {
        LOG(INFO) << "Loaded graph to vineyard: " << fragment_group_id;

        auto pg = get_partitioned_graph_by_object_id(client, fragment_group_id);
        auto local_partitions = grin_get_local_partition_list(pg);
        size_t pnum = grin_get_partition_list_size(local_partitions);
        assert(pnum > 0);

        // we only sync the first partition as example
        auto partition = grin_get_partition_from_list(local_partitions, 0);
        sync_property(pg, partition, "likes", "features");
    }
```

## GRIN Concepts

This section explains the key concepts in GRIN to help users understand GRIN APIs more easily.

### Predefined Macros

GRIN provides a set of predefined C macros for storage engines to describe their features. When a storage engine implements the GRIN APIs, it should first set up the macros to present its features such as partition strategies and enabled indices.

The benefit is two-fold:

1. On the computing engine side, developers can implement their methods using alternative logic and GRIN APIs when some features are claimed or disclaimed by different storage engines. This makes the code switch based on storage feature claims happens in the compiling stage rather than in runtime. Thus, it extends the versatility of methods implemented by GRINs without sacrificing efficiency.

2. On the storage engine side, the predefined macros can filter out a large number of unnecessary APIs to avoid developers implementing them with boilerplate or inefficient code, since the design of storage engines may differ enormously from each other.

What follows is an example of predefined macros regarding the locally completeness of vertex properties.

- Four macros are provided:
    1. `GRIN_ASSUME_ALL_VERTEX_PROPERTY_LOCAL_COMPLETE`
    2. `GRIN_ASSUME_MASTER_VERTEX_PROPERTY_LOCAL_COMPLETE`
    3. `GRIN_ASSUME_BY_TYPE_ALL_VERTEX_PROPERTY_LOCAL_COMPLETE`
    4. `GRIN_ASSUME_BY_TYPE_MASTER_VERTEX_PROPERTY_LOCAL_COMPLETE`
- Some assumptions may dominate others, which means that some assumptions apply in a wider range than others. Therefore, storage providers should be careful when setting these assumptions. Here, 1 dominates the others, which means that 2 to 4 are undefined when 1 is defined. Additionally, 2 dominates 4, and 3 dominates 4.
- GRIN provides different APIs under different assumptions. Suppose only 3 is defined; it means that vertices of certain types have all the properties locally complete, regardless of whether the vertex is master or mirror. In this case, GRIN provides an API to return these locally complete vertex types. 
- In the case that none of these four macros is defined, GRIN will provide a per-vertex API to tell whether the vertex property is locally complete.


### Partition Strategy
GRIN provides two types (i.e., edge-cut and vertex-cut) of predefined partition strategies. Each strategy can be seen as a set of granular predefined macros based on the common understanding of the partition strategy. For storages using hybrid partition strategy, developers can set up the granular macros one after another.

#### Edge-cut Partition Strategy

- Vertex data is locally complete for master vertices.
- Edge data is locally complete for all edges.
- Neighbors are locally complete for master vertices.
- Vertex properties are locally complete for master vertices.
- Edge properties are locally complete for all edges.

#### Vertex-cut Partition Strategy

- Vertex data is locally complete for all vertices.
- Edge data is locally complete for all edges.
- Mirror partition list is available for master vertices to broadcast messages.
- Vertex properties are locally complete for all vertices.
- Edge properties are locally complete for all edges.

### Property Graph Model
GRIN makes the following assumptions for its property graph model.
- Vertices have types, as do edges. 
- The relationship between edge types and pairs of vertex types is many-to-many.
- Properties are bound to vertex and edge types, but some properties may have the same name.
- Labels can be assigned to vertices and edges (**NOT** their types) primarily for query filtering, and labels have no properties.


## Implementation Guideline

### For computing engine

- Get GRIN APIs from [GRIN](https://github.com/GraphScope/GRIN)
- Implement a wrapper class (normally `grin_fragment` or `grin_graph`) using GRIN APIs.
- When you find some function is hard or inefficient to implement, discuss with GRIN designers.
- Write or modify apps using the wrapper class (e.g., `grin_fragment`).
- Find a storage implementation to set up an end-to-end test.

### For storage engine
- Make a grin folder in your system, and navigate into the grin folder.
- Add GRIN as a submodule and copy the `predefine.template` out as the `predefine.h` 
```console
    $ git submodule add https://github.com/GraphScope/GRIN.git include

    $ cp include/predefine.template predefine.h
```
- Modify the StorageSpecific part in the `predefine.h` based on the features of the storage.
- Implement the headers as much as possible in another folder (e.g., src) under grin. 
If you find the time complexity of some function is **NOT** sub-linear to the graph size,
discuss with GRIN designers.
- Write a storage-specific method to get a graph handler from your storage.
- Run a graph traversal test.


## Design Details
### Handler
- GRIN provides a series of handlers for graph concepts, such as vertex, edge and graph itself. 
- Since almost everything in GRIN are handlers except of only a few string names, the type for a graph concept and its handler is always mixed-used in GRIN.
- For example, GRIN uses the type Vertex to represent the type of a vertex handler, instead of using VertexHandler for clean code.

### Edge
- For any directed edge `(u, v)` where the direction goes from `u` to `v`, `u` is the source vertex, whereas `v` is the destination vertex.

### List
GRIN provides two alternative approaches, namely array-style retrieval and iterator, for list retrieval of `GRIN_VERTEX_LIST`, `GRIN_EDGE_LIST`, and `GRIN_ADJACENT_LIST`. 

For other schema-level lists like `GRIN_VERTEX_TYPE_LIST` or `GRIN_PARTITION_LIST`, GRIN generally assume the array-style list retrieval, and does **NOT** provide a `GRIN_ENABLE_` macro for these lists.

#### Array-style Retrieval
- The array-style retrieval of a list handler is available to the user only if the storage can provide the size of the list, and an element retrieval API by position (i.e., index of array). Otherwise, the storage should provide a list iterator, see next section.
- Usually the array-style retrieval is controlled by the macros ended with `_LIST_ARRAY`
- A vertex list array-style retrieval example

    ```CPP
        /* grin/topology/vertexlist.h */

        GRIN_VERTEX_LIST grin_get_vertex_list(GRIN_GRAPH g);  // get the vertexlist of a graph

    #ifdef GRIN_ENABLE_VERTEX_LIST_ARRAY
        size_t grin_get_vertex_list_size(GRIN_GRAPH g, GRIN_VERTEX_LIST vl);  // implement the API to return the size of vertexlist

        GRIN_VERTEX grin_get_vertex_from_list(GRIN_GRAPH g, GRIN_VERTEX_LIST vl, size_t idx);  // implement the API to return the element of vertexlist by position
    #endif

        /* run.cc */
        {
            auto vertexlist = grin_get_vertex_list(g); // with a graph (handler) g
            auto sz = grin_get_vertex_list_size(g, vertexlist);

            for (auto i = 0; i < sz; ++i) {
                auto v = grin_get_vertex_from_list(g, vertexlist, i);
            }
        }
    ```

#### List Iterator
- A list iterator handler is provided to caller if the list size is unknown or for sequential scan efficiency. 
- Usually the iterators are enabled by macros ended with `_LIST_ITERATOR`
- A vertex list iterator example

    ```CPP
        /* grin/topology/vertexlist.h */

    #ifdef GRIN_ENABLE_VERTEX_LIST_ITERATOR
        GRIN_VERTEX_LIST_ITERATOR grin_get_vertex_list_begin(GRIN_GRAPH g, GRIN_VERTEX_LIST vl);  // get the begin iterator of the vertexlist

        GRIN_VERTEX_LIST_ITERATOR grin_get_next_vertex_list_iter(GRIN_GRAPH g, GRIN_VERTEX_LIST_ITERATOR vli);  // get next iterator

        bool grin_is_vertex_list_end(GRIN_GRAPH g, GRIN_VERTEX_LIST_ITERATOR vli); // check if reaches the end

        GRIN_VERTEX grin_get_vertex_from_iter(GRIN_GRAPH g, GRIN_VERTEX_LIST_ITERATOR vli); // get the vertex from the iterator
    #endif

        /* run.cc */
        {
            auto iter = grin_get_vertex_list_begin(g, vl); // with a graph (handler) g and vertexlist vl

            while (!grin_is_vertex_list_end(g, iter)) {
                auto v = grin_get_vertex_from_iter(g, iter);
                iter = grin_get_next_vertex_list_iter(g, iter);
            }
        }
    ```

### Property
- Properties are bound to vertex and edge types. It means even some properties may have the same name, as long as they are bound to different vertex or edge types, GRIN will provide distinct handlers for these properties. This is
because, although properties with the same name usually provide the same semantic in the graph, they may have 
different data types in the underlying storage for efficiency concerns (e.g., short date and long date).
- To avoid the incompatibility with storage engines, we made the design choice to bind properties under vertex and edge types. Meanwhile, GRIN provides an API to get all the property handlers with the (same) given property name.

    ```CPP
        /* grin/property/type.h */

        GRIN_VERTEX_TYPE grin_get_vertex_type_by_name(GRIN_GRAPH g, const char* name);


        /* grin/property/property.h */

        GRIN_VERTEX_PROPERTY grin_get_vertex_property_by_name(GRIN_GRAPH g, GRIN_VERTEX_TYPE vtype, const char* name);

        GRIN_VERTEX_PROPERTY_LIST grin_get_vertex_properties_by_name(GRIN_GRAPH g, const char* name);


        /* run.cc */
        {
            auto vtype = grin_get_vertex_type_by_name(g, "Person");  // get the vertex type of Person
            auto vprop = grin_get_vertex_property_by_name(g, vtype, "Name");  // get the Name property bound to Person

            auto vpl = grin_get_vertex_properties_by_name(g, "Name");  // get all the properties called Name under all the vertex types (e.g., Person, Company) in g
        }
    ```

### Label
- GRIN does **NOT** distinguish label on vertices and edges, that means a vertex and an edge may have a same label.
- However the storage can tell GRIN whether labels are enabled in vertices or edges seperatedly with macros of `WITH_VERTEX_LABEL` and `WITH_EDGE_LABEL` respectively.

### Order
- GRIN provides sorted vertex list assumptions. 
- GRIN also assumes that if a vertex list sorted, then there is complete ordering for the vertices from the list.
- Sorted vertex list faciliates computations like vertex list join and data structures like vertex array which uses vertex as the index of the array.

### Reference
- GRIN introduces the reference concept in partitioned graph. It stands for the reference of an instance that can
be recognized in partitions other than the current partition where the instance is accessed.
- For example, a `GRIN_VERTEX_REF` is a reference of a `GRIN_VERTEX` that can be recognized in other partitions.

    ```CPP
        /* grin/partition/partition.h */
        
        GRIN_VERTEX_REF grin_get_vertex_ref_for_vertex(GRIN_GRAPH, GRIN_VERTEX);
        
        const char* grin_serialize_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF);

        GRIN_VERTEX_REF grin_deserialize_to_vertex_ref(GRIN_GRAPH, const char*);

        GRIN_VERTEX grin_get_vertex_from_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF);


        /* run.cc in machine 1 */
        {
            auto vref = grin_get_vertex_ref_for_vertex(g, v);  // get v's vertex ref which can be recgonized in machine 2

            const char* msg = grin_serialize_vertex_ref(g, vref);  // serialize into a message

            // send the message to machine 2...
        }


        /* run.cc in machine 2 */
        {
            // recieve the message from machine 1...

            auto vref = grin_deserialize_to_vertex_ref(g, msg);  // deserialize back to vertex ref

            auto v = grin_get_vertex_from_vertex_ref(g, vref);  // cast to vertex if g can recognize the vertex ref
        }
    ```

### Master and Mirror
- Master & mirror vertices are the concept borrowed from vertexcut partition strategy. When a vertex is recognized in
serveral partitions, GRIN refers one of them as the master vertex while others as mirrors. This is primarily for data
aggregation purpose to share a common centural node for every one.
- While in edgecut partition, the concept becomes inner & outer vertices. GRIN uses master & mirror vertices to represent inner & outer vertices respectively to unify these concepts.

### Local Complete
- The concept of local complete is with repect to whether a graph component adhere to a vertex or an edge is locally complete within the partition.
- Take vertex and properties as example. GRIN considers the vertex is "property local complete" if it can get all the properties of the vertex locally in the partition.
- There are concepts like "edge property local complete", "vertex neighbor local complete" and so on.
- GRIN does **NOT** assume any local complete on master vertices. Since in some extremely cases, master vertices
may **NOT** contain all the data or properties locally.
- GRIN currently provides vertex-level/edge-level local complete judgement APIs, while the introduction of type-level judgement APIs is open for discussion.

### Natural ID Trait
- Concepts represent the schema of the graph, such as vertex type and properties bound to a certain edge type, are usually numbered naturally from `0` to its `num - 1` in many storage engines. To facilitate further optimizations
in the upper computing engines, GRIN provides the natural number ID trait. A storage can provide such a trait if
it also uses the natural numbering for graph schema concepts.

