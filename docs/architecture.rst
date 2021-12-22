# Architecture of GraphScope
## Components
GraphScope’s major components include `GraphScope Client`, `GraphScope Coordinator`, `Graph Analytical Engine`, `Graph Interactive Engine` and `Graph Learning Engine`. It uses `vineyard` for zero-copy in-memory data sharing across engines, and `vineyard` uses `etcd` for global key-value storage and synchronization.

## Standalone mode
Standalone mode aims on easier to getting started, handling small datasets that can fit into the memory of a single machine (A gentle description)

### Launching procedure

User can launch a local session explicitly by
```python
import graphscope
sess = graphscope.session(cluster_type='hosts')
```
or implicitly by just create a graph
```python
import graphscope
g = graphscope.g()
```
They both will launch a local session, in which the client will launch the coordinator, and coordinator is responsible for the other components.


TODO: Add image

### Details introduction of the session parameters

### How builtin datasets works

## Cluster mode
Cluster modes leverage Kubernetes to scale out, enable users to handle super large graphs (billions) with lightning speed. 

### Launching procedure

### How builtin datasets works
