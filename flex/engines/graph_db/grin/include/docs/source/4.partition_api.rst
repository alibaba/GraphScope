Partition APIs
--------------

Vertex Reference
^^^^^^^^^^^^^^^^
``GRIN_VERTEX_REF`` is a reference to a vertex in the graph. It is used to exchange vertex information 
with remote partitions.

To get the ``vertex_ref`` of a ``vertex`` and vice-versa, use:

::

    GRIN_VERTEX_REF grin_get_vertex_ref_by_vertex(GRIN_GRAPH, GRIN_VERTEX);

    GRIN_VERTEX grin_get_vertex_by_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF);

Since ``GRIN_VERETX_REF`` is still a handle, we can further serialize it into ``const char*`` or
``int64`` if ``GRIN_TRAIT_FAST_VERTEX_REF`` is defined.

::

    const char* grin_serialize_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF); 

    #ifdef GRIN_TRAIT_FAST_VERTEX_REF
    long long int grin_serialize_vertex_ref_as_int64(GRIN_GRAPH, GRIN_VERTEX_REF);
    #endif

Accordingly, the ``vertex_ref`` can be deserialized from ``const char*`` or ``int64``.

::

    GRIN_VERTEX_REF grin_deserialize_vertex_ref(GRIN_GRAPH, const char*);

    #ifdef GRIN_TRAIT_FAST_VERTEX_REF
    GRIN_VERTEX_REF grin_deserialize_int64_to_vertex_ref(GRIN_GRAPH, long long int);
    #endif

Users can also get the master partition of a vertex using its vertex reference.

:: 

    GRIN_PARTITION grin_get_master_partition_from_vertex_ref(GRIN_GRAPH, GRIN_VERTEX_REF);

Actually vertex reference implies a vertex partition protocol between partitions.


Select Master
^^^^^^^^^^^^^
In partitioned graph, a common need is to select master vertices from a vertex list.
Particularly in edgecut, this stands for the ``inner`` vertices of a partition.

GRIN provides related APIS to handle this if corresponding traits are defined.

::

    GRIN_VERTEX_LIST vlist = grin_get_vertex_list(g);

    #ifdef GRIN_TRAIT_SELECT_MASTER_FOR_VERTEX_LIST
    GRIN_VERTEX_LIST master_vlist = grin_select_master_for_vertex_list(g, vlist);

    grin_destroy_vertex_list(g, master_vlist);
    #endif

    grin_destroy_vertex_list(g, vlist);
