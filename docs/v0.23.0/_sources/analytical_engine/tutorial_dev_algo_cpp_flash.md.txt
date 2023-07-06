# Tutorial: Develop Your Algorithms in C++ with FLASH Model

We have implemented over 70 graph algorithms with FLASH for over 40 different common problems. These algorithms cover various areas such as clustering, centrality, traversal, matching, mining, etc. We continuously add more algorithms to the FLASH library and encourage users to develop their algorithms using the FLASH model if the built-in algorithms do not meet their requirements.

## Developing Algorithms with FLASH Model

### Defining A FLASH Application

[FLASH](https://graphscope.io/docs/latest/analytical_engine/flash.html) is a programming model specifically designed for writing distributed graph processing algorithms. It follows the Bulk Synchronous Parallel (BSP) computing paradigm and provides primary functions like *VSize*, *VertexMap*, and *EdgeMap*, which are defined on the *VertexSubset* structure, and a series of auxiliary functions like set operations. The FLASH model provides strong expressiveness and makes it possible to program more advanced algorithms than existing vertex-centric algorithms.

To implement your algorithms using the FLASH model, you need to define a FLASH application first, by  fulfilling an APP class in C++. If the result of the algorithm is a value on each vertex, the class looks like:

```cpp
template <typename FRAG_T>
class YourApp : public FlashAppBase<FRAG_T, V_TYPE> {
 public:
  INSTALL_FLASH_WORKER(YourApp<FRAG_T>, V_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, V_TYPE, V_RES_TYPE>;

  bool sync_all_ = false; // true or false

  V_RES_TYPE* Res(V_TYPE* v) {
    // return the result on the vertex
  }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw, ...) {
    // conduct processing using the FLASH APIs
  }
};
```

Or, if the result of the algorithm is a single global value, the class looks like:

```cpp
template <typename FRAG_T>
class YourApp : public FlashAppBase<FRAG_T, V_TYPE> {
 public:
  INSTALL_FLASH_WORKER(YourApp<FRAG_T>, V_TYPE, FRAG_T)
  using context_t = FlashGlobalDataContext<FRAG_T, V_TYPE, G_RES_TYPE>;

  bool sync_all_ = false; // true or false

  G_RES_TYPE Res() {
    // return the global result
  }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw, ...) {
    // conduct processing using the FLASH APIs
  }
};
```

### Implementing the Run Function

After that, a user can implement a distributed graph processing algorithm through utilizing various FLASH APIs in the Run Function of the application. Let's take BFS as an example, an user-defined application may be like this in file `GraphScope/analytical_engine/apps/flash/traversal/bfs.h`:

```cpp
template <typename FRAG_T>
class BFSFlash : public FlashAppBase<FRAG_T, BFS_TYPE> {
 public:
  INSTALL_FLASH_WORKER(BFSFlash<FRAG_T>, BFS_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, BFS_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->dis); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw,
           oid_t o_source) {
    vid_t source = Oid2FlashId(o_source);
    DefineMapV(init) { v.dis = (id == source) ? 0 : -1; };
    vset_t a = VertexMap(All, CTrueV, init);

    DefineFV(filter) { return id == source; };
    a = VertexMap(a, filter);

    DefineMapE(update) { d.dis = s.dis + 1; };
    DefineFV(cond) { return v.dis == -1; };
    for (int len = VSize(a), i = 1; len > 0; len = VSize(a), ++i) {
      a = EdgeMap(a, ED, CTrueE, update, cond);
    }
  }
};
```
As shown in this code, to implement a BFS algorithm, users only need to design and implement processing operations on the *VertexSubset*, rather than considering communication, message passing and graph partitioning in a distributed setting. This reduces the burden of programming various kinds of algorithms.

### Integrating with the Python Client of GraphScope

To run your FLASH algorithms from the Python client of GraphScope, one needs to provide a corresponding Python function defined for this algorithm. For example, to integrate the above BFS algorithm, one can define a function as follows in the file `GraphScope/python/graphscope/analytical/app/flash/traversal.py`.

```python
@project_to_simple
@not_compatible_for("arrow_property", "dynamic_property")
def bfs(graph, source=1):
    """Evaluate BFS on a graph with flash computation mode.

    Args:
        graph (:class:`graphscope.Graph`): A simple graph.
        source (optional): The source Node. Defaults to 1.

    Returns:
        :class:`graphscope.framework.context.Context`:
            A context with each vertex assigned with the distance from source.
    """
    return AppAssets(algo="flash_bfs", context="vertex_data")(graph, source)
```

In addition, the configuration information needs to be added in `GraphScope/coordinator/gscoordinator/builtin/app/.gs_conf.yaml`. Then the algorithm could be invoked from the Python client.

```yaml
- algo: flash_bfs
  type: cpp_flash
  class_name: gs::BFSFlash
  src: apps/flash/traversal/bfs.h
  compatible_graph:
    - grape::ImmutableEdgecutFragment
    - gs::ArrowProjectedFragment
```

### Running the FLASH Algorithms

To run your own FLASH algorithms, you may trigger it from the client of GraphScope once you have installed and deployed GAE. At first, a property graph will be loaded into GraphScope.

```python
# Import the graphscope module.

import graphscope

graphscope.set_option(show_log=True)  # enable logging
```

```python
# Load p2p network dataset

from graphscope.dataset import load_p2p_network

graph = load_p2p_network(directed=True)
```

The FLASH algorithms are defined on a simple graph, which has only one kind of vertices and edges, edges and vertices have at most one property as their attribute. Therefore, one needs to project a property graph to convert it to a simple graph, by selecting one kind of label for vertices/edges, and each with at most one of their properties.

```python
simple_graph = graph.project(vertices={"host": []}, edges={"connect": ["dist"]})
```

Then, we can run the BFS algorithm implemented in the FLASH model, by taking the projected graph in the previous step as the input parameter, and setting the source node id to be "1". The algorithm will codegen a compatible version for the loaded graph, and compile to an executable binary. It may take a bit longer to build the library. However, this step only takes once for the same algorithm on a typed graph.

```python
context = graphscope.flash.bfs(simple_graph, source=1)
```

After the computation, the results are distributed on the vineyard instances on the cluster. The returned object is a Context, which has several methods to retrieve or persist the results.
In this case, the results represent the distance from the source node. We use the following code to fetch the results and display them with their vertex id.

```python
context.to_dataframe(
	selector={"id": "v.id", "dist": "r"}, vertex_range={"begin": 1, "end": 10}
).sort_values(by="id")
```
