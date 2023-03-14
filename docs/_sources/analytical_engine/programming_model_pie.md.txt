# Programming Model: PIE

Although the [vertex-centric programming model](https://graphscope.io/docs/latest/analytical_engine/vertex_centric_models.html) can express various graph analytics algorithms, existing sequential (single-machine) graph algorithms have to be modified to comply with the “think like a vertex” principle, making parallel graph computation a privilege for experienced users only. In addition, the performance of graph algorithms with vertex-centric model is sub-optimal in many cases: each vertex only has information about its 1-hop neighbors, and thus information is propagated through the graph slowly, one hop at a time. As a result, it may take many computation iterations to propagate a piece of information from a source to a destination.

## What is the PIE Model?

To address the abovementioned problems, we proposed a new programming model PIE (PEval-IncEval-Assemble) in a [SIGMOD paper](https://dl.acm.org/doi/10.1145/3035918.3035942) published in 2017. Different from the vertex-centric, the PIE model can automatically parallelize existing sequential graph algorithms, with only some small changes. This makes parallel graph computations accessible to users who know conventional graph algorithms covered in college textbooks, and there is no need to recast existing graph algorithms into a new model.

Specifically, in the PIE model, users only need to provide three functions,

- (1) *PEval*, a sequential (single-machine) function for given a query, computes the answer on a local partition;
- (2) *IncEval*, a sequential incremental function, computes changes to the old output by treating incoming messages as updates; and
- (3) *Assemble*, which collects partial answers, and combines them into a complete answer.

:::{figure-md}

<img src="../images/pie.png"
     alt="The PIE model"
     width="80%">

The PIE model. 
:::

## Workflow of PIE

The PIE model works on a graph *G* and each worker maintains a partition of *G*. Given a query, each worker first executes *PEval* against its local partition, to compute partial answers in parallel. Then each worker may exchange partial results with other workers via synchronous message passing. Upon receiving messages, each worker incrementally computes *IncEval*. The incremental step iterates until no further messages can be generated. At this point, *Assemble* pulls partial answers and assembles the final result. In this way, the PIE model parallelizes existing sequential graph algorithms, without revising their logic and workflow.

In this model, users do not need to know the details of the distributed setting while processing big graphs in a cluster, and the PIE model auto-parallelizes the graph analytics tasks across
a cluster of workers, based on a fixpoint computation. Under a monotonic condition, it guarantees to
converge with correct answers as long as the three sequential algorithms provided are correct.

The following pseudo-code shows how SSSP is expressed in the PIE model, where the Dijkstra’s algorithm is directly used for the computation of parallel SSSP.

```python
def dijkstra(g, vals, updates):
    heap = VertexHeap()
    for i in updates:
	vals[i] = updates[i]
	heap.push(i, vals[i])
        
    updates.clear()
    
    while not heap.empty():
	u = heap.top().vid
	distu = heap.top().val
	heap.pop()
	for e in g.get_outgoing_edges(u):
	    v = e.get_neighbor()
	    distv = distu + e.data()
	    if vals[v] > distv:
		vals[v] = distv
		    if g.is_inner_vertex(v):
			heap.push(v, distv)
			updates[v] = distv       
	return updates

def PEval(source, g, vals, updates):
    for v in vertices:
        updates[v] = MAX_INT
    updates[source] = 0
    dijkstra(g, vals, updates)
    
def IncEval(source, g, vals, updates):
    dijkstra(g, vals, updates)
```






TODO(wanglei): Under the hook
    - MessageBuffer,
    - ParallelExecutor,