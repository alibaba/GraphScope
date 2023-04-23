# Getting Started

This guide gives you a quick start to use GraphScope for graph analysis tasks on your local machine.

## Installation

We’ll start by installing GraphScope with a single-line command.

```bash

python3 -m pip install graphscope --upgrade

```

````{tip}
If you occur a very low downloading speed, try to use a mirror site for the pip.

```bash
python3 -m pip install graphscope --upgrade \
    -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com
```
````


## Running GraphScope Analytical Engine on Local

The `graphscope` package includes everything you need to analysis a graph on your local machine.
Now you may import it in a Python session and start your job.

```python

import graphscope as gs
from graphscope.dataset.modern_graph import load_modern_graph

gs.set_option(show_log=True)

# load the modern graph as example.
#(modern graph is an example property graph for Gremlin queries given by Apache at https://tinkerpop.apache.org/docs/current/tutorials/getting-started/)
graph = load_modern_graph()

# triggers label propagation algorithm(LPA)
# on the modern graph(property graph) and print the result.
ret = gs.lpa(graph)
print(ret.to_dataframe(selector={'id': 'v.id', 'label': 'r'}))


# project a modern graph (property graph) to a homogeneous graph
# and run single source shortest path(SSSP) algorithm on it, with assigned source=1.
pg = graph.project(vertices={'person': None}, edges={'knows': ['weight']})
ret = gs.sssp(pg, src=1)
print(ret.to_dataframe(selector={'id': 'v.id', 'distance': 'r'})

```

## What's the Next

As shown in the above example, it is very easy to use GraphScope to analyze a graph with our provided algorithms on your local machine.
Next, you may want to learn more about the following topics:

- [Design of the analytical engine of GraphScope and its technical details](analytical_engine/design_of_gae)
- [Disaggregate deployment of GraphScope on a k8s cluster for large-scale graph analysis](analytical_engine/deployment)
- [A set of examples with advanced usage, including customized algorithms, NetworkX/Giraph/GraphX compatibility, etc.](analytical_engine/guide_and_exmaples)
