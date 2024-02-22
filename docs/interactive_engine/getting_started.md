# Getting Started

This guide gives you a quick start to use GraphScope for graph interactive tasks on your local machine.

## Installation

GIE works as the core component of GraphScope. Therefore, you should install GraphScope at first to use GIE.

GraphScope requires the following software versions:

- Python 3.9 ~ 3.11
- JDK 11 (Both JDK 8 and 20 have known compatibility issues)
- gcc 7.1+ or Apple Clang


And it is tested on the following 64-bit operating systems:

- Ubuntu 18.04 or later
- CentOS 7 or later
- macOS 12 (Intel/Apple silicon) or later, with both Intel chip and Apple M1 chip

If your environment satisfies the above requirement, you can easily install GraphScope through pip:

```bash
python3 -m pip install graphscope --upgrade
```

````{tip}
If the download is very slow, try to use a mirror site for the pip.

```bash
python3 -m pip install graphscope --upgrade \
    -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host=mirrors.aliyun.com
```
````

However, GraphScope requires specific versions for some packages, so you may meet some conflicts and errors during the installation. Therefore, we strongly recommend you to **install GraphScope in a clean Python virtual environment with Python 3.9**.

We refer you to [the official guide](https://docs.python.org/3.9/tutorial/venv.html) for Python virtual environment, and the following is a step by step instruction:

```bash
# Create a new virtual environment
python3.9 -m venv tutorial-env
# Activate the virtual environment
source tutorial-env/bin/activate
# Install GraphScope
python3.9 -m pip install graphscope
# Use GraphScope
python3.9
>>> import graphscope as gs
>>> ......
```

## Running GraphScope Interactive Engine on Local

It's fairly straightforward to run interactive queries using the `graphscope` package on
your local machine. First of all, you import `graphscope` in a Python session, and load
the modern graph, which has been widely used in [Tinkerpop](https://tinkerpop.apache.org/docs/3.6.2/tutorials/getting-started/) demos.


```python
import graphscope as gs
from graphscope.dataset.modern_graph import load_modern_graph

gs.set_option(show_log=True)

# load the modern graph as example.
graph = load_modern_graph()

# Hereafter, you can use the `graph` object to create an `interactive` query session
g = gs.interactive(graph)
# then `execute` any supported gremlin query (by default)
q1 = g.execute('g.V().count()')
print(q1.all().result())   # should print [6]

q2 = g.execute('g.V().hasLabel(\'person\')')
print(q2.all().result())  # should print [[v[2], v[3], v[0], v[1]]]

# or `execute` any supported Cypher query, by passing `lang="cypher"`
q3 = g.execute("MATCH (n) RETURN count(n)", lang="cypher")
print(q3.records[0][0])  # should print 6
```

You may see something like:
```Shell
...
... [INFO][coordinator:453]: Built interactive frontend xxx.xxx.xxx.xxx:pppp for graph xxx
[6]
...
[v[2], v[3], v[0], v[1]]
...
```

The number 6 is printed, which is the number of vertices in modern graph.

### Customize Configurations for GIE instance

You could pass additional key-value pairs to customize the startup configuration of GIE, for example:

```python
# Set the timeout value to 10 min
g = gs.interactive(graph, params={'query.execution.timeout.ms': 600000})
```

## What's the Next
As shown in the above example, it is very easy to use GraphScope to interactively query a graph using both the Gremlin and Cypher query language on your local machine.

In addition to the above local-machine entr\'ee, we have prepared the following topics for your reference.

- GIE can be [deployed standalone](./deployment.md) in a distributed environment to process very large graph.
- GIE has been designed to [integrate with the Tinkerpop ecosystem](./tinkerpop/tinkerpop_gremlin.md), with necessary extensions such as some [syntactic sugars](./tinkerpop/supported_gremlin_steps.md) to ease the use of Gremlin.
- GIE has been designed to [integrate with the Neo4j ecosystem](./neo4j/cypher_sdk.md).
- Learn more about the [design of GIE](./design_of_gie).
