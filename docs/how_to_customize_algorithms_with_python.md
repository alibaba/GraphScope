# How to Customize Algorithms with Python

GraphScope provides a Python SDK to allow users to develop their customized algorithms, where both PIE and Pregel programming models are supported. In this document, we introduce how to develop algorithms with PIE and Pregel models respectively. 

## Writing Your Own Algorithms in PIE
Users may write their own algorithms if the built-in algorithms do not meet their needs. GraphScope enables users to write algorithms in the PIE programming model in a pure Python mode.

To implement this, a user just need to fulfill this class.

```python
from graphscope.analytical.udf.decorators import pie
from graphscope.framework.app import AppAssets

@pie(vd_type="double", md_type="double")
class YourAlgorithm(AppAssets):
    @staticmethod
    def Init(frag, context):
        pass

    @staticmethod
    def PEval(frag, context):
        pass

    @staticmethod
    def IncEval(frag, context):
        pass
```

As shown in the code, users need to implement a class decorated with @pie and provides three sequential graph functions. In the class, the Init is a function to set the initial status. PEval is a sequential method for partial evaluation, and IncEval is a sequential function for incremental evaluation over the partitioned fragment. The full API of fragment can be found in [Cython SDK API](https://graphscope.io/docs/reference/cython_sdk.html#cython-sdk-api).

Let’s take SSSP as example, a user defined SSSP in PIE model may be like this.

```python
from graphscope.analytical.udf.decorators import pie
from graphscope.framework.app import AppAssets

@pie(vd_type="double", md_type="double")
class SSSP_PIE(AppAssets):
    @staticmethod
    def Init(frag, context):
        v_label_num = frag.vertex_label_num()
        for v_label_id in range(v_label_num):
            nodes = frag.nodes(v_label_id)
            context.init_value(
                nodes, v_label_id, 1000000000.0, PIEAggregateType.kMinAggregate
            )
            context.register_sync_buffer(v_label_id, MessageStrategy.kSyncOnOuterVertex)

    @staticmethod
    def PEval(frag, context):
        src = int(context.get_config(b"src"))
        graphscope.declare(graphscope.Vertex, source)
        native_source = False
        v_label_num = frag.vertex_label_num()
        for v_label_id in range(v_label_num):
            if frag.get_inner_node(v_label_id, src, source):
                native_source = True
                break
        if native_source:
            context.set_node_value(source, 0)
        else:
            return
        e_label_num = frag.edge_label_num()
        for e_label_id in range(e_label_num):
            edges = frag.get_outgoing_edges(source, e_label_id)
            for e in edges:
                dst = e.neighbor()
                # use the third column of edge data as the distance between two vertices
                distv = e.get_int(2)
                if context.get_node_value(dst) > distv:
                    context.set_node_value(dst, distv)

    @staticmethod
    def IncEval(frag, context):
        v_label_num = frag.vertex_label_num()
        e_label_num = frag.edge_label_num()
        for v_label_id in range(v_label_num):
            iv = frag.inner_nodes(v_label_id)
            for v in iv:
                v_dist = context.get_node_value(v)
                for e_label_id in range(e_label_num):
                    es = frag.get_outgoing_edges(v, e_label_id)
                    for e in es:
                        u = e.neighbor()
                        u_dist = v_dist + e.get_int(2)
                        if context.get_node_value(u) > u_dist:
                            context.set_node_value(u, u_dist)
```

As shown in the code, users only need to design and implement sequential algorithm over a fragment, rather than considering the communication and message passing in the distributed setting. In this case, the classic dijkstra algorithm and its incremental version works for large graphs partitioned on a cluster.

## Writing Algorithms in Pregel

In addition to the sub-graph based PIE model, graphscope supports vertex-centric Pregel model as well. You may develop an algorithms in Pregel model by implementing this.

```python
from graphscope.analytical.udf.decorators import pregel
from graphscope.framework.app import AppAssets

@pregel(vd_type='double', md_type='double')
class YourPregelAlgorithm(AppAssets):

    @staticmethod
    def Init(v, context):
        pass

    @staticmethod
    def Compute(messages, v, context):
        pass

    @staticmethod
    def Combine(messages):
        pass
```

Differ from the PIE model, the decorator for this class is @pregel. And the functions to be implemented is defined on vertex, rather than the fragment. Take SSSP as example, the algorithm in Pregel model looks like this.

```python
from graphscope.analytical.udf import pregel
from graphscope.framework.app import AppAssets

# decorator, and assign the types for vertex data, message data.
@pregel(vd_type="double", md_type="double")
class SSSP_Pregel(AppAssets):
    @staticmethod
    def Init(v, context):
        v.set_value(1000000000.0)

    @staticmethod
    def Compute(messages, v, context):
        src_id = context.get_config(b"src")
        cur_dist = v.value()
        new_dist = 1000000000.0
        if v.id() == src_id:
            new_dist = 0
        for message in messages:
            new_dist = min(message, new_dist)
        if new_dist < cur_dist:
            v.set_value(new_dist)
            for e_label_id in range(context.edge_label_num()):
                edges = v.outgoing_edges(e_label_id)
                for e in edges:
                    v.send(e.vertex(), new_dist + e.get_int(2))
        v.vote_to_halt()

    @staticmethod
    def Combine(messages):
        ret = 1000000000.0
        for m in messages:
            ret = min(ret, m)
        return ret
```