# Performance Tuning

Memory footprint and performance on large-scale graph data are the keys
to the success of graph analysis in real-world scenarios. In this section,
We'll go through the internal design of the property graph data structure
in GraphScope, analyze the impact factor of memory footprint and performance,
and finally give some suggestions on how to optimize the performance and
reduce the memory usage of graph analysis.

Memory Footprint of Property Graphs
-----------------------------------

We first dive into the detailed design of the property graph data structure
to see how it is stored in memory and which factors affect the memory footprint.

### Property graph data structure

GraphScope uses the `ArrowFragment` data structure [defined in Vineyard][1] for
its property graphs. Basically, the `ArrowFragment` has the following members:


- Indexers: the vertices in the user input graphs are natural integral numbers
  or strings. To make the graph analytical processing efficient, we need to map
  the original IDs in the user input to a consecutive range of integral numbers.
  This process requires a data structure called `VertexMap`, which is basically
  a hashmap which maps the original vertex ID to the internal vertex ID and an
  array which record the original vertex IDs in each partition.

  - `o2g_<fragment_id>_<vertex_label>`: vertices in each partition for each label
    has such a hashmap. The hashmap is either flatten hashmap or perfect hashmap.

    The key type of the hashmap is the same with the original vertex IDs (usually
    `int64_t` or `std::string_view`) and the value type is the internal vertex ID
    (usually `uint64_t`).

  - `oid_arrays_<fragment_id>_<vertex_label>`: arrays for original vertex IDs in
    each partition for each label.

    The type of this array is the same with the original vertex IDs (usually `int64_t`
    or `string`).

- Topologies: the first major part of the property graph is the topology: it basically
  a CSR (Compressed Sparse Row Format) matrix:

    - incoming edges: each `(src_type, edge_type)` pair has a CSR matrix for its incoming
      edges. The CSR matrix consists of a `indptr` array and a `indices` array:
      - `ie_lists_-<vertex_label>-<edge_label>`: the `indptr` array, each element in
        the `indptr` array is a `(neighbor_vertex_id, edge_table_index)` pair where the
        first the neighbor vertex id and the second is the index points to the
        corresponding edge table.

        By default, the type of `neighbor_vertex_id` is `uint64_t` or `uint32_t` and
        the type of `edge_table_index` is `size_t`.

        The size of the `indptr` array is `num_edges`.

      - `ie_offsets_lists_-<vertex_label>-<edge_label>`: the `indices` array, each
        element in the `indices` array is an `offset`, and the slice
        `ie_lists[ie_offsets[i]:ie_offsets[i+1]]` is the edges for vertex `i`.

        By default, the type of `offset` is `size_t`.

        The size of `indices` array is `num_vertices + 1`, which is a 0-based offset array.

    - outgoing edges: a CSR matrix, same as the incoming edges, but for outgoing edges
      of current partition.

      - `oe_lists_-<vertex_label>-<edge_label>`: the `indptr` array, each element in
        the `indptr` array is a `(neighbor_vertex_id, edge_table_index)` pair where the
        first the neighbor vertex id and the second is the index points to the
        corresponding edge table.

        By default, the type of `neighbor_vertex_id` is `uint64_t` or `uint32_t` and
        the type of `edge_table_index` is `size_t`.

        The size of the `indptr` array is `num_edges`.

      - `oe_offsets_lists_-<vertex_label>-<edge_label>`: the `indices` array, each
        element in the `indices` array is an `offset`, and the slice
        `oe_lists[oe_offsets[i]:oe_offsets[i+1]]` is the edges for vertex `i`.

        By default, the type of `offset` is `size_t`.

        The size of `indices` array is `num_vertices + 1`, which is a 0-based offset array.

- Properties: the second part of the property graph is the properties: each vertex
  label and each edge label has a table for its properties:

  - `edge_tables_-<edge_label>`: tables for edge properties, each edge label has such
     a table;
  - `vertex_tables_-<vertex_label>`: tables for vertex properties, each vertex label has
    such a table.

### Memory usage estimation

The memory usage of a given fragment with vertex number `V`, edge number `E`, original
ID type `OID_T` and internal ID type `VID_T` can be
estimated as:

- Indexers:

  - with flatten hashmap: `(sizeof(OID_T) + sizeof(VID_T) + sizeof(uint8_t)) * V / load_factor`;

    From the observation in our practices, the `load_factory` is usually within the range
    of `[0.4, 0.5]`.

  - with perfect hashmap: `(sizeof(OID_T) * V) * (1 + overhead)`.

    In practice the `overhead` is usually within the range of `[0.15, 0.2]`.

- Topologies:

  - incoming edges: `(sizeof(VID_T) + sizeof(size_t)) * E + sizeof(size_t) * (V+1)`;
  - outgoing edges: same as incoming edges, `(sizeof(VID_T) + sizeof(size_t)) * E + sizeof(size_t) * (V+1)`.

- Properties:

  - edge properties: depends on how many edge properties you have.

    In GraphScope, by default the an extra column `edge_id` property (of type `int64_t`)
    will be generated and added to the edge table as a unique identifier for each edge.

  - vertex properties: depends on how many vertex properties you have.

    In GraphScope, by default the original vertex ID is kept as a property in the vertex table
    as well.

Optimizing Memory Usage
-----------------------

Based on the above analysis, we summary the optimization tips of reducing fragment memory
footprint as follows:

- Optimizing indexers:

  - Use perfect hashmap. It is not the default option but can be enabled by the argument
    `use_perfect_hash=True` in `graphscope.g()` and `graphscope.load_from()`.

    As analyzed above, the perfect hashmap can reduce the memory footprint of vertex map
    for a really large margin.

  - Use local vertex map. GraphScope internally has two kinds of vertex map implemented, the
    former is called `GlobalVertexMap` which stores all vertices in all fragments in the
    indexer, the later is called `LocalVertexMap` which only stores related vertices (vertices that
    has edges between inner vertices of current fragment) in the indexer.

    The `LocalVertexMap` is not the default option but can be enabled by the argument
    `vertex_map="local"` in `graphscope.load_from()`. The `LocalVertexMap` is suitable for
    graphs which will scales to many nodes (e.g., dozens or hundreds of workers), but it
    does has some limitations on the flexibility that can only used when loading graphs using
    `graphscope.load_from()` and repeatedly `add_vertices/edges()` are not supported.

- Optimizing topologies:

  - GraphScope uses `uint64_t` as the `VID_T` (internal vertex id) to support large-scale
    graphs. However, from above analysis, the type of `VID_T` is one of the key factors
    that affects the memory footprint of the topology part.

    If you are sure your graph is fairly small (less than `10^8` of vertices, the absolute
    value depends on number of labels and number of partitions), you can use `int32_t`
    as the `VID_T` to optimize the memory usage, by `vid_type="int32_t"` option in
    `graphscope.g()` and `graphscope.load_from()`.s

  - GraphScope supports options `compact_edges=True` in `graphscope.g()` and `graphscope.load_from()`
    to compact the `ie_lists` and `oe_lists` arrays using delta and varint encoding. Such compression
    can half the memory footprint of the topology part, but has overhead in computation during
    traversing the fragment that can up to `20%`.

- Optimize properties:

  - The generation of `edge_id` column in the edge tables can be avoided by the argument
    `generated_eid=False` in `graphscope.g()` and `graphscope.load_from()`. This helps a
    a lot (saves `sizeof(size_t) * E`) if your edges doesn't much many properties and you
    only need to run efficient analytical jobs.

    Note that if you intend to run interactive queries on the graph, the argument `generated_eid`
    must be `True`.

  - The preservation of `vertex_id` column in the vertex tables can be avoided by the argument
    `retain_oid=False` in `graphscope.g()` and `graphscope.load_from()`. It helps not very much
    (saves `sizeof(OID_T) * V`) but the gain can be more significant if your graph has low `E/V`
    ratio (graphs has many vertices and not so many edges).

    Note that if you intend to run interactive queries on the graph, the argument `retain_oid`
    must be `True`.

Optimizing Performance of Graph Analytics
-----------------------------------------

GraphScope supports analytical applications on both `ArrowFragment` graphs with many vertex
labels, edge labels, and properties, as well as `ArrowProjectedFragment` graphs with only
one vertex label, one edge label, and at most one property for each vertex and edge.

- The analytical applications on `ArrowFragment` requires an implicit "flatten" process to
  make it a `ArrowFlattenFragment`.

  The `ArrowFlattenFragment` can be thought as a "view" on the property graph `ArrowFragment`.
  It is mainly for compatibility purpose and has performance penalty for traversing.
  In practice if the performance of analytical applications is critical, flatten fragments
  should be avoid and projected fragments should be used instead.

- The analytical applications on `ArrowProjectFragment` requires an implicit "project" process
  to create the `ArrowProjectedFragment`. This process involves traversing the edges and
  generating a new `offsets` arrays.

  To optimizing the run time in cases where you need to run many different algorithms on the
  same graph using the same projection settings, it is preferred to project the fragment
  explicitly to `ArrowProjectFragment` first to avoid the overhead of the "project" process. i.e.,

  Instead of:

  ```python
  g = ....   # fragment that can be implicit projected

  r1 = sssp(g, src=1)
  r2 = pagerank(g)
  r3 = wcc(g)
  ...
  ```

  You should first project it explicitly:

  ```python
  g = ....   # fragment that can be implicit projected

  projected_g = g._project_to_simple()
  r1 = sssp(projected_g, src=1)
  r2 = pagerank(projected_g)
  r3 = wcc(projected_g)
  ```

When apply analytical algorithms on `ArrowFragment`, if (1) it has only one vertex label, (2)
it has only one edge label, and (3) each vertex and edge has at most one property, then
the `ArrowProjectedFragment` will be generated, otherwise, the `ArrowFlattenFragment` will be used.

[1]: https://github.com/v6d-io/v6d/blob/main/modules/graph/fragment/arrow_fragment.vineyard-mod
