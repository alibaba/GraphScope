# Tutorial: LDBC Gremlin
Prior to beginning, it is important to note that [LDBC](https://ldbcouncil.org/) is a highly
regarded organization in the realm of graph databases/systems benchmark standards and audit testing.
LDBC has developed several benchmark toolsuits, with GIE primarily focusing on the
[Social Network Benchmark (SNB)](https://ldbcouncil.org/benchmarks/snb/). This benchmark defines
graph workloads specifically for database management systems, including both interactive (high-QPS)
workloads and business intelligence workloads.

During this tutorial, we will walk you through various use cases when querying the simulated
social network utilized in SNB. By the conclusion of this tutorial, we are confident that
you will be proficient in constructing Gremlin queries that are just as intricate as the
business intelligence workloads found in SNB.

## Load the LDBC Graph
For this tutorial, we will use the [LDBC social network](https://ldbcouncil.org/) as the graph data, which has the following entities:

- vertex/edge labels(types) in the graph
- connections and relationships among labels
- properties related all kinds of labels

:::{figure-md}

<img src="../images/LDBC_Schema.png"
     alt="The schema of the LDBC graph"
     width="65%">

Figure 1. The schema of the LDBC graph (referred to LDBC [SNB](https://ldbcouncil.org/benchmarks/snb/)).
:::

LDBC graph is one of the built-in dataset of GraphScope, so it can be easily loaded through:

```python
from graphscope.dataset.ldbc import load_ldbc

graph = load_ldbc()
```

This will load the LDBC social network with the scale factor (sf) 1.

```{tip}
We do permit loading much larger LDBC graphs, such as sf 3k.
To handle such large graphs, you're recommended using the [standalone deployment of GIE](./deployment.md)
in a large cluster.
```

Currently, GIE supports Gremlin as its query language.
After loading the LDBC graph and initializing the engine, we can submit gremlin queries to GIE through `g.execute(GREMLIN_QUERIES)` easily.

For example, if we want to count there are how many vertices and edges in the LDBC graph, we can simply write the following python codes:

```python
import graphscope as gs
from graphscope.dataset.ldbc import load_ldbc

# load the ldbc graph
graph = load_ldbc()

# Hereafter, you can use the `graph` object to create an `gremlin` query session
g = gs.gremlin(graph)
# then `execute` any supported gremlin query.
# count vertices
q1 = g.execute('g.V().count()')
print(q1.all())
# count edges
q2 = g.execute('g.E().count()')
print(q2.all())
```

Then the output should be:

```bash
[190376]
[787074]
```

It suggests that there are 190376 vertices and 787074 edges in the LDBC sf1 graph.

## Basic Vertex/Edge Query

It is very common to use SQL sentences to retrieve data from relational databases. In graph, vertices can be regarded as entities, and edges can be regarded as the connecting relationship among entities. Similarly, we can easily retrieve data from graph with GIE and corresponding graph query sentences.



### Retrieve Vertices and Edges

As shown in the last tutorial, we can easily retrieve vertices and edges from graph through `g.V()` and `g.E()` gremlin steps.

```python
# then `execute` any supported gremlin query.
# Retrieve all vertices
q1 = g.execute('g.V()')
print(q1.all())
# Retrieve all edges
q2 = g.execute('g.E()')
print(q2.all())
```

The output of the above code should be like:

```bash
# All vertices
[v[432345564227583365], ......, v[504403158265495622]]
# All edges
[e[576460752303435306][432345564227579434-hasType->504403158265495612], ......, e[144115188075855941][504403158265495622-isSubclassOf->504403158265495553]]
```

For vertices, the number inside `[]` represents its id. However, the ids seem to be a little bit confusing: why are they so large? Actually, such large ids result from the different storage mechanism between graph and relational databases.

In relational databases, instances with different types are stored separately in different tables. Therefore, we usually use *local id* to retrieve specific instance from the table. For example, we can use `select * from Person where id=0` to retrieve person with id=0 from the Person table. Since the id is local, instances belonging to different tables may have the same id (e.g. person(id=0) and message(id=0)).

When it comes to graph storage, all kinds of vertices and edges are stored together. If it still uses local id to locate vertices and edges, there will be many conflicts. Therefore, every vertex and edge needs a specific global identifier, called *global id*, to distinguish it from others.

GIE uses *global id* to identify very vertex and edge. The ids look so large mainly because the underlying storage generates vertex's  *global id* from its  *local id* and *label* by some bit operations(shift and plus). After all, the *global ids* are just identifiers, and we don't need to worry too much about what it looks like.

For edges, still the number inside the first `[]` represents its id. The content inside the second `[]` is of the format `source vertex id -edge label(type)-> destination vertex id`，indicating the edge's two end vertices' id and the connection type (edge label).

### Apply Some Filters

In most cases, we don't need to retrieve all the vertices and edges from the graph, as we may only care about a small portion of them. Therefore, GIE with gremlin provides you with some filtering operations to help find the data you are interested in.

If you only want to extract a vertex/edge with the given id, you can write gremlin sentence `g.V(id)` in GIE. (`g.E(id)` will be supported in the future)

```python
# Retrieve vertex with id 1
q1 = g.execute('g.V(1)')
print(q1.all())
```

The output should be like:

```bash
[v[1]]
```

Sometimes, you may want to retrieve vertices/edges having a specific label, you can use `haveLabel(label)` step in gremlin. For example, the following codes show how to find all person vertices.

```python
# Retrieve vertices having label 'person'
q1 = g.execute('g.V().hasLabel(\'person\')')
print(q1.all())
```

The output should be like:

```bash
# All person veritces
[v[216172782113783808], ......, v[216172782113784710]]
```

If you want to extract more than one type(label) of vertices, you can write many labels in the step, like `haveLabel(label1, label2...)`. For example, the following codes extracts all the vertices having label 'person' or 'forum'.

```python
# Retrieve vertices having label 'person' or 'forum'
q1 = g.execute('g.V().hasLabel(\'person\', \'forum\')')
print(q1.all())
```

The output should be like:

```bash
# All person veritces and forum vertices
[v[216172782113783808], ......,  v[72057594037936036]]
```

As mentioned in the last tutorial, GraphScope models data graph as property graph, so GIE allows user to filter the outputs according to some properties by using `has(...)` step.

The usage of `has(...)` step is very flexible. Firstly, it allows user to find vertices having some required properties. For example, the following codes aims to extract vertices having property 'creationDate'

```python
# Retrieve vertices having property 'creationDate'
q1 = g.execute('g.V().has(\'creationDate\')')
print(q1.all())
```

The output should be like:

```bash
# All vertices having property 'creationDate'
v[360287970189718653], ......, v[360287970189718655]]
```

From the LDBC schema shown above, you can see that vertices with label 'person', 'forum',  'post' and 'comment' all have property 'creationDate'. Therefore, the above query sentence is equivalent to:

```python
# Retrieve vertices having label 'person' or 'forum' or 'message'
q1 = g.execute('g.V().hasLabel(\'person\', \'forum\', \'comment\', \'post\')')
print(q1.all())
```

In addition, you may further require the extracted properties satisfying some conditions. For example, if you want to extract persons whose first name is 'Joseph', you can write the following codes:

```python
# Retrieve person vertices whose first name is 'Joeseph'
q1 = g.execute('g.V().hasLabel(\'person\').has(\'firstName\', \'Joseph\')')
print(q1.all())
```

You can also write more complicated predicates in the `has(...)` step, for example:

```python
# Retrieve person vertices whose first name is not 'Joseph'
q1 = g.execute('g.V().hasLabel(\'person\').has(\'firstName\', not(eq(\'Joseph\')))')
# Retrieve person vertices whose first name is either 'Joseph' or 'Yacine'
q2 = g.execute('g.V().hasLabel(\'person\').has(\'firstName\', within(\'Joseph\', \'Yacine\'))')
# Retrieve comment vertices created after the very beginning of year 2011
q3 = g.execute('g.V().hasLabel(\'comment\').has(\'creationDate\', gt( \'2011-01-01T00:00:00.000+0000\'))')
print(q1.all())
print(q2.all())
print(q3.all())
```

Here are more [references](https://tinkerpop.apache.org/docs/3.6.2/reference/#has-step) about `has(...)` step in gremlin.

### Extract Property Values

Sometimes, you may more curious about the property values of the vertices/edges. With GIE, you can easily  use `values(PROPERTY_NAME)` gremlin step to extract the vertices/edges' property value.

For example, you have already known that vertex(id=38416) is a comment, and you are interested in its content, then you can write the following codes in GIE:

```python
# Extract comment vertex(id=38416)'s content
q1 = g.execute('g.V(38416).values(\'content\')')
print(q1.all())
```

The output should be like:

```bash
# The content of the comment(id=38416)
['maybe']
```

### Real Applications

Since GIE provides python interface, it is very convenient to do some further data analytics on the retrieved data.

Here is an example about the statistics of the comment content's length (word count) in the LDBC Graph.

```python
import matplotlib.pyplot as plt
import graphscope as gs
from graphscope.dataset.ldbc import load_ldbc

# load the modern graph as example.
graph = load_ldbc()

# Hereafter, you can use the `graph` object to create an `gremlin` query session
g = gs.gremlin(graph)

# Extract all comments' contents
q1 = g.execute('g.V().hasLabel(\'comment\').values(\'content\')')
comment_contents = q1.all()
comment_length = [len(comment_content.split()) for comment_content in comment_contents];

# Draw Histogram
plt.hist(x=list(filter(lambda x:(x< 50), comment_length)), bins='auto', color='#607c8e',alpha=0.75)
plt.grid(axis='y', alpha=0.75)
plt.xlabel('Comments Length')
plt.ylabel('Comments Count')
plt.title('Comment Length Histogram')
plt.show()
```

:::{figure-md}

<img src="../images/comments_length_histogram.png"
     alt="The_histogram"
     width="65%">

 Figure 2. The histogram of the comments' length in the ldbc social network.
:::

We can also draw a pie chart about the gender ratio in the social network.

```python
import matplotlib.pyplot as plt
import graphscope as gs
from graphscope.dataset.ldbc import load_ldbc


# load the modern graph as example.
graph = load_ldbc()

# Hereafter, you can use the `graph` object to create an `gremlin` query session
g = gs.gremlin(graph)

# Extract all person' gender property value
q1 = g.execute('g.V().hasLabel(\'person\').values(\'gender\')')
person_genders = q1.all()
# Count male and female
male_count = 0
female_count = 0
for gender in person_genders:
    if gender == "male":
        male_count += 1
    else:
        female_count += 1

# Draw pie chart
plt.pie(x=[male_count, female_count], labels=["Male", "Female"])
plt.show()
```

:::{figure-md}

<img src="../images/gender_ratio.png"
     alt="The_pie_chart"
     width="60%">

Figure 3. The pie chart of person's gender in the ldbc social network.
:::

## Basic Traversal Query

The main difference between Property Graph and Relational Database is that Property Graph treats the relationship(edges) among entities(vertices) at the first class. Therefore, it is easy to walk through vertices and edges in the Property Graph according to some pre-defined paths.

In GIE, we call such walk as  **Graph Traversal**: This type of workload involves traversing the graph from a set of source vertices while satisfying the constraints on the vertices and edges that the traversal passes. Graph traversal differs from the [analytics](https://graphscope.io/docs/latest/graph_analytics_workloads.html) workload as it typically accesses a small portion of the graph rather than the whole graph.

In this tutorial, we would like to discuss how to use gremlin steps to traverse the Property Graph. Furthermore, we will show some examples about how to apply graph traversals in real world data analytics.

### Expansion

The most basic unit in Graph Traversal is Expansion, which means starting from a vertex/edge and reaching its adjacencies. GIE currently supports the following expansion steps in gremlin:

- `out()`: Map the vertex to its outgoing adjacent vertices given the edge labels.
- `in()`: Map the vertex to its incoming adjacent vertices given the edge labels.
- `both()`: Map the vertex to its adjacent vertices given the edge labels.
- `outE()`: Map the vertex to its outgoing incident edges given the edge labels.
- `inE()`: Map the vertex to its incoming incident edges given the edge labels.
- `bothE()`: Map the vertex to its incident edges given the edge labels.
- `outV()`: Map the edge to its outgoing/tail incident vertex.
- `inV()`: Map the edge to its incoming/head incident vertex.
- `otherV()`: Map the edge to the incident vertex that was not just traversed from in the path history.
- `bothV():`Map the edge to its incident vertices.

Here is a local subgraph extracted from the LDBC Graph, which is formed by a center person vertex (id=216172782113784483) and all of its adjacent edges and vertices. Then we will use this subgraph to explain all of the expansion steps.


:::{figure-md}
<img src="../images/loacal_graph_example.png"
     alt="loacal_graph_example"
     width="33%">

Figure 5. A local graph around a given person vertex.
:::

#### out(), in() and both()

All of these three steps start traverse from source vertices to their adjacent vertices. The differences between them are:

- `out()` traverse through the **outgoing** edges to the adjacent vertices
- `in()` traverse through the **incoming** edges to the adjacent vertices
- `both()` traverse through **both of the outgoing and incoming** edges to the adjacent vertices

For example, if you want to find vertex(id=216172782113784483)'s adjacent vertices by the three steps, you can write sentences in GIE like:

```python
# Traverse from the vertex to its adjacent vertices through its outgoing edges
q1 = g.execute('g.V(216172782113784483).out()')
# Traverse from the vertex to its adjacent vertices through its incoming edges
q2 = g.execute('g.V(216172782113784483).in()')
# Traverse from the vertex to its adjacent vertices through all of its incident edges
q3 = g.execute('g.V(216172782113784483).both()')
print(q1.all())
print(q2.all())
print(q3.all())
```

This figure illustrates the execution process of `q1`, `q2` and `q3`:

:::{figure-md}

<img src="../images/out_in_both.png"
     alt="out_in_both"
     width="95%">

Figure 6. The examples of `out`/`in`/`both` steps.
:::

Therefore, the output of the above codes should be like:

```bash
# q1: vertex's adjacent vertices through outgoing edges
[v[432345564227569033], v[288230376151712472], v[144115188075856168], v[144115188075860911]]
# q2: vertex's adjacent vertices through incoming edges
[v[72057594037934114]]
# q3: vertex's adjacent vertices through all of its incident edges
[v[432345564227569033], v[288230376151712472], v[144115188075856168], v[144115188075860911], v[72057594037934114]]
```

In addition, the three steps all support using edge labels as its parameters to further limit the traversal edges, like `out`/`in`/`both(label1, label2, ...)`, which means it only can traverse through the edges whose label is one of {label 1, label 2, ..., label n}. For example:

```python
# Traverse from the vertex to its adjacent vertices through its incoming edges, and the edge label should be 'hasModerator'
q1 = g.execute('g.V(216172782113784483).in(\'hasModerator\')')
# Traverse from the vertex to its adjacent vertices through its outgoing edges, and the edge label should be either 'studyAt' or 'workAt'
q2 = g.execute('g.V(216172782113784483).out(\'studyAt\', \'workAt\')')
# Traverse from the vertex to its adjacent vertices through all of its incident edges, and the edge label should be either 'isLocatedIn' or 'hasModerator'
q3 = g.execute('g.V(216172782113784483).both(\'isLocatedIn\', \'hasModerator\')')
print(q1.all())
print(q2.all())
print(q3.all())
```

This figure illustrates the execution process of `q1`, `q2` and `q3`:

:::{figure-md}
<img src="../images/out_in_both_labels.png"
     alt="out_in_both_labels"
     width="95%">

Figure 7. The examples of `out`/`in`/`both` steps from given labels.
:::


Therefore, the output of the above codes should be like:

```bash
# q1: vertex's adjacent vertices through incoming 'hasModerator' edges
[v[72057594037934114]]
# q1: vertex's adjacent vertices through outgoing 'studyAt' or 'workAt' edges
[v[144115188075860911], v[144115188075856168]]
# q1: vertex's adjacent vertices through 'isLocatedIn' or 'hasModerator' edges
[v[288230376151712472], v[72057594037934114]]
```

#### outE(), inE() and bothE()

All of these three steps start traverse from source vertices to their **adjacent edges**. Similar to `out()`, `in()` and `both()`, the differences between them are:

- `outE()` can only traverse to the **outgoing**  adjacent edges of the source vertices
- `inE()` can only traverse to the **incoming**  adjacent edges of the source vertices
- `bothE()` can traverse to **both of the outgoing and incoming** adjacent edges of the source vertices

For example, if you want to find vertex(id=216172782113784483)'s adjacent edges by the three steps, you can write sentences in GIE like:

```python
# Traverse from the vertex to its outgoing adjacent edges
q1 = g.execute('g.V(216172782113784483).outE()')
# Traverse from the vertex to its incoming adjacent edges
q2 = g.execute('g.V(216172782113784483).inE()')
# Traverse from the vertex to all of its adjacent edges
q3 = g.execute('g.V(216172782113784483).bothE()')
print(q1.all())
print(q2.all())
print(q3.all())
```

This figure illustrates the execution process of `q1`, `q2` and `q3`:

:::{figure-md}

<img src="../images/outE_inE_bothE.png"
     alt="The current states of graph queries"
     width="95%">

Figure 8. The examples of `outE`/`inE`/`bothE` steps.
:::

Therefore, the output of the above codes should be like:

```bash
# q1: vertex's incident outgoing edges
[e[432345564227582847][216172782113784483-hasInterest->432345564227569033], e[504403158265496227][216172782113784483-isLocatedIn->288230376151712472], e[864691128455136658][216172782113784483-workAt->144115188075856168], e[1008806316530991636][216172782113784483-studyAt->144115188075860911]]
# q2: vertex's incident incoming edges
[e[360287970189645858][72057594037934114-hasModerator->216172782113784483]]
# q3: vertex's incident edges
[e[360287970189645858][72057594037934114-hasModerator->216172782113784483], e[432345564227582847][216172782113784483-hasInterest->432345564227569033], e[504403158265496227][216172782113784483-isLocatedIn->288230376151712472], e[864691128455136658][216172782113784483-workAt->144115188075856168], e[1008806316530991636][216172782113784483-studyAt->144115188075860911]]
```

Similarly, the three steps also support using edge labels as its parameters to further limit the traversal edges, like `outE/inE.bothE(label1, label2, ...)`, which means it only can traverse to the edges whose label is one of {label 1, label 2, ..., label n}. For example:

```python
# Traverse from the vertex to its incident incoming edges, and the edge label should be 'hasModerator'
q1 = g.execute('g.V(216172782113784483).inE(\'hasModerator\')')
# Traverse from the vertex to its incident outgoing edges, and the edge label should be either 'studyAt' or 'workAt'
q2 = g.execute('g.V(216172782113784483).outE(\'studyAt\', \'workAt\')')
# Traverse from the vertex to its incident edges, and the edge label should be either 'isLocatedIn' or 'hasModerator'
q3 = g.execute('g.V(216172782113784483).bothE(\'isLocatedIn\', \'hasModerator\')')
print(q1.all())
print(q2.all())
print(q3.all())
```

This figure illustrates the execution process of `q1`, `q2` and `q3`:

:::{figure-md}
<img src="../images/outE_inE_bothE_labels.png"
     alt="outE_inE_bothE_labels"
     width="95%">

Figure 9. The examples of `outE`/`inE`/`bothE` steps from given labels.
:::

Therefore, the output of the above codes should like:

```bash
# q1: vertex's incident incoming 'hasModerator' edges
[e[360287970189645858][72057594037934114-hasModerator->216172782113784483]]
# q2: vertex's incident outgoing 'studyAt' or 'workAt' edges
[e[1008806316530991636][216172782113784483-studyAt->144115188075860911], e[864691128455136658][216172782113784483-workAt->144115188075856168]]
# q3: vertex's incident 'isLocatedIn' or 'hasModerator' edges
[e[360287970189645858][72057594037934114-hasModerator->216172782113784483], e[504403158265496227][216172782113784483-isLocatedIn->288230376151712472]]
```

#### outV(), inV(), bothV() and otherV()

When reaching an edge during the traversal, you may be interested in its incident vertices. Therefore, GIE supports gremlin steps that traverse from source edges to their incident vertices.

:::{figure-md}
<img src="../images/out_in_vertices.png"
     alt="out_in_vertices"
     width="50%">

Figure 10. The examples of outgoing/incoming vertex of an edge.
:::


For en edge `e1` whose direction is from `v1` to `v2`, we call `v1` as 'outgoing vertex' and `v2` as 'incoming' vertices.

- `outV()`: traverse from source edges to the outgoing vertices
- `inV()`: traverse from source edges to the incoming vertices
- `bothV()`: traverse from source edges to both of the outgoing and incoming vertices
- `otherV()`: this step is a little bit special, it traverse from source edges to the incident vertices that haven't appear in the traversal history. It will be explained in details later.

In the local subgraph, assume that currently we are at 'isLocatedIn' edge. If you want to get its incident vertices through `outV()/inV()/bothV()` step, you can write sentences in GIE like:

```python
# Traverse from the edge to its outgoing incident vertex
q1 = g.execute('g.V(216172782113784483).outE(\'isLocatedIn\').outV()')
# Traverse from the edge to its incoming incident vertex
q2 = g.execute('g.V(216172782113784483).outE(\'isLocatedIn\').inV()')
# Traverse from the edge to both of its incident vertex
q3 = g.execute('g.V(216172782113784483).outE(\'isLocatedIn\').bothV()')
print(q1.all())
print(q2.all())
print(q3.all())
```

This figure illustrates the execution process of `q1`, `q2` and `q3`:

:::{figure-md}

<img src="../images/outV_inV_bothV.png"
     alt="outV_inV_bothV"
     width="95%">

Figure 11. The examples of outV/inV/bothV steps.
:::

Therefore, the output of the above codes should like:

```bash
# q1: the edge's outgoing vertex
[v[216172782113784483]]
# q2: the edge's incoming vertex
[v[288230376151712472]]
# q3: both of the edge's outgoing and incoming vertices
[v[216172782113784483], v[288230376151712472]]
```

For `otherV()`, it will only traverse to the vertices that haven't been reached in the traversal history. For example, the 'isLocatedIn' edge is reached from the outgoing vertex 'Person', then the `otherV()`  of 'isLocatedIn' edge is its incoming vertex 'Place', because it hasn't been visited before.

```python
# Traverse from the edge to its incident vertex that hasn't been reached
q1 = g.execute('g.V(216172782113784483).outE(\'isLocatedIn\').otherV()')
print(q1.all())
```

The output should be like:

```bash
# q1: 'place' vertex, which has not been reached before
[v[288230376151712472]]
```

#### Multiple Expansion Steps

You may have already noticed that graph traversal supports multiple expansion steps. For example, in gremlin sentence`g.V(216172782113784483).outE('isLocatedIn').inV()`, `outE('isLocatedIn')` is first expansion and `inV()` is the second expansion.

The figure illustrates the execution process of this gremlin sentence:

:::{figure-md}

<img src="../images/outE_then_inV.png"
     alt="outE_then_inV"
     width="70%">

Figure 12. The examples of `outE` followed by `inV`.
:::

During the traversal, it firstly start from the source vertex 'person', and get to the 'isLocatedIn' edge. Then it starts from the 'isLocatedIn' edge, and get to the 'place' vertex. Thus the traversal ends, the 'place' vertex is the end of this traversal, and the 'isLocatedIn' edge can be regarded as the intermediate process of the traversal.

When GIE handle continuous multiple expansion steps during the traversal, the next expansion step will use the its previous expansion step's output as its starting points. According to this property, we can summarize the following equivalent rules:

- `out() = outE().inV() = outE().otherV()`
- `in() = inE().outV() = inE().otherV()`
- `both() = bothE().otherV()`

It is sure that GIE can support multiple vertex expansion steps. For example, the place (city) the person is located in 'isPartOf' a larger place (country). If you want to find the larger place, you can write multiple vertex expansion steps in GIE as:

```python
# Traverse from the vertex to the larger place it is located in by two vertex expansions
q1 = g.execute('g.V(216172782113784483).out(\'isLocatedIn\').out(\'isPartOf\')')
print(q1.all())
```

This figure illustrates the execution process:

:::{figure-md}
<img src="../images/out_out_predicate.png"
     alt="out_out_predicate"
     width="40%">

Figure 13. The examples of two-hop outgoing vertices.
:::

Therefore, the output of the gremlin sentence should be like:

```bash
# The larger place(country) that the person is located in
[v[288230376151711797]]
```

There is one problem of currently introduced gremlin traversal sentences: only the last step's results are kept. Sometimes, you may need to record the intermediate results of the traversal for further analysis, but how? Actually, you can use the `as(TAG)` and `select(TAG 1, TAG 2, ..., TAG N)` steps to keep the intermediate results:

- `as(TAG)`: it gives a tag to the step it follows, and then its previous step's value can be accessed through the tag.
- `select(TAG 1, TAG 2, ..., TAG N)`: select all the values of the steps the given tags refer to.

Extend from previous example, if you want to keep person, smaller place(city) and larger place(country) together in the output, you can write gremlin sentence in GIE like:

```python
# a: person, b: city, c: country
q1 = g.execute('g.V(216172782113784483).as(\'a\')\
                 .out(\'isLocatedIn\').as(\'b\')\
                 .out(\'isPartOf\').as(\'c\')\
                 .select(\'a\', \'b\', \'c\')')
print(q1.all())
```

where tag 'a' refers to 'person' vertex, tag 'b' refers to 'city' vertex and tag 'c' refers to 'country' vertex. The output should be like:

```bash
[{'a': v[216172782113784483], 'b': v[288230376151712472], 'c': v[288230376151711797]}]
```

#### Expansion from many starting points

Currently, we only discuss the expansion from only one vertex/edges. However, it is very common to write gremlin sentence like `g.V().out()` in GIE. In previous tutorials we have known that `g.V()` will get all the vertices in the graph. Then how to understand the meaning of `g.V().out()`?

The explain it more clear, let's firstly look at a much simpler situation: starting from only two vertices. This is a subgraph of LDBC Graph formed by two person vertices' local graphs, where the two person vertices' ids are 216172782113784483 and 216172782113784555.

:::{figure-md}

<img src="../images/two_local_graphs.png"
     alt="two_local_graphs"
     width="80%">

Figure 14. The examples of two local graphs.
:::

In addition, we further limit the starting points of the traversal to be exactly the two person vertices. Therefore, we can write the following gremlin sentence in GIE:

```python
# Traverse from the two source vertices to their adjacent vertices through their incident outgoing edges
q1 = g.execute('g.V(216172782113784483, 216172782113784555).out()')
print(q1.all())
```

The figure illustrates the execution process of `q1`:

:::{figure-md}

<img src="../images/out_two_starting_points.png"
     alt="two_local_graphs"
     width="80%">

Figure 15. The results of outgoing neighbors from two given vertices.
:::

The traversal starts from the two person vertices, and then the `out()` step map the two source vertices both to their outgoing adjacent vertices. Therefore, the output should be like:

```bash
[v[432345564227569033], v[288230376151712472], v[144115188075856168], v[144115188075860911], v[432345564227569357], v[432345564227570524], v[288230376151712984], v[144115188075861043]]
```

which are the outgoing adjacent vertices of the two 'person' vertices.

It is more clear to understand if you keep person vertex's info in the final output:

```python
q1 = g.execute('g.V(216172782113784483, 216172782113784555).as(\'a\')\
                 .out().as(\'b\')\
                 .select(\'a\', \'b\')')
print(q1.all())
```

```bash
# the first 4 elements are tuple of vertex(id=216172782113784483) and its outgoing adjacent vertex
# the last 4 elements are tuple of vertex(id=216172782113784555) and its outgoing adjacent vertex
[{'a': v[216172782113784483], 'b': v[432345564227569033]},
 {'a': v[216172782113784483], 'b': v[288230376151712472]},
 {'a': v[216172782113784483], 'b': v[144115188075856168]},
 {'a': v[216172782113784483], 'b': v[144115188075860911]},
 {'a': v[216172782113784555], 'b': v[432345564227569357]},
 {'a': v[216172782113784555], 'b': v[432345564227570524]},
 {'a': v[216172782113784555], 'b': v[288230376151712984]},
 {'a': v[216172782113784555], 'b': v[144115188075861043]}]
```

Generally, it is better to understand the expansion and traversal step from traverser's aspect. Every expansion step actually map current traversers to another series traversers, and the complete traversal can be regarded as the transformations of traversers.  Let's take `g.V().out().in()` as an example to explain it.

- At the very beginning, `g.V()` extracts all the vertices from the graph, and these vertices form the initial traversers: each traverser contains the corresponding vertex.
- Then `out()` maps every traverser (vertex) to its outgoing adjacent vertices, and these outgoing adjacent vertices form the next series of traversers.
- Finally, `in()` maps every traverser (vertex) to its incoming adjacent vertices, and these incoming adjacent vertices form the next series of traversers, and it is the final output of the gremlin sentence.

This figure illustrates the overview of the traverser transformation during the traversal of `g.V().out().in()`.

:::{figure-md}
<img src="../images/traversers_transformation_overview.png"
     alt="traversers_transformation_overview"
     width="50%">

Figure 16. The overview of traversers' transformation.
:::

In addition, this figure shows the details of the traverser's change after every expansion step and `as(TAG)` step during the execution of gremlin sentence `g.V().as('a').out().as('b').in().as('c')`.

:::{figure-md}
<img src="../images/traversers_transformation_details.png"
     alt="traversers_transformation_details"
     width="70%">

Figure 17. The mapping details of the above traversers' transformation.
:::


### Filter & Expansion

#### Apply filters after expansion

In last tutorial, we have discussed that we can use `has(...)` series of steps to apply filters to the extracted vertices and edges. Actually, these steps can be applied after expansion steps either that filter on the current traversers.

Still using `g.V().out().in()` as the example, if we hope that:

- The traversers(vertices) after `g.V()` has label 'person'

-  The traversers(vertices) after the first expansion `out()` have property 'browserUsed' and the value is 'Chrome'
- The traversers(vertices) after the second expansion `in()` have property 'length' and the length is < 5, and the label of the expansion edge is 'replyOf'

```python
# Traversers(vertices) after g.V() has label 'person'
# Traversers(vertices) after out() has property 'browserUsed' and the value is 'Chrome'
# Traversers(vertives) after in('replyOf') has property 'length' and the length is < 5
q1 = g.execute('g.V().hasLabel(\'person\')\
                     .out().has(\'browserUsed\', \'Chrome\')\
                     .in(\'replyOf\').has(\'length\', lt(5))')
print(q1.all())
```

The output should be like:

```bash
[v[54336], ..., v[33411]]
```

Furthermore, for gremlin sentences contain `as(TAG)` steps, we can introduce `where()` step to apply filters on traversers based on the tagged values.

Extend from previous example that we add tag 'a' and 'b' after the two `has(...)` step:

```python
q1 = g.execute('g.V().hasLabel(\'person\')\
                     .out().has(\'browserUsed\', \'Chrome\').as(\'a\')\
                     .in(\'replyOf\').has(\'length\', lt(5)).as(\'b\')')
```

If we hope that the vertices of the tag 'a' and 'b' have the same property value on property 'browserUsed', we can add a `where` step and write the following gremlin sentence in GIE:

```python
# Traversers(vertices) after g.V() has label 'person'
# Traversers(vertices) after out() has property 'browserUsed' and the value is 'Chrome'
# Traversers(vertives) after in('replyOf') has property 'length' and the length is < 5
# vertex a and b have the same value of 'browserUsed' property
q1 = g.execute('g.V().hasLabel(\'person\')\
                     .out().has(\'browserUsed\', \'Chrome\').as(\'a\')\
                     .in(\'replyOf\').has(\'length\', lt(5)).as(\'b\')\
                     .where("b", eq("a")).by("browserUsed")\
                     .select(\'a\', \'b\')')
print(q1.all())
print(q1.all())
```

The output should be like:

```bash
[{'a': v[360287970189700805], 'b': v[59465]}, ..., {'a': v[33403], 'b': v[33411]}]
```

#### Expansion as filters

In addition, not only can we add filters after expansion steps, but also we can use expansion step as the filtering predicate with `where(...)` step. For example, if we want to extract all the vertices having at least 5 outgoing incident edges, we can write a gremlin sentence with `where(...)` step as following:

```python
# Retrieve vertices having at least 5 outgoing incident edges
q1 = g.execute('g.V().where(outE().count().is(gte(5)))')
print(q1.all())
```

### Path Expansion (Syntactic Sugar)

Until now, we can use expansion steps, filter steps and auxiliary steps like `as(TAG)` to write complicated gremlin sentences to traverse the graph. However, there are still two shortcomings:

- If we want to find a vertex which is 10-hop(edges) away from the current vertex, we need to write expansion step 10 times, which is very cost-ineffective.
- The number of hops from the source to the destination is arbitrary. For example, if we want to find all vertices which can be reached from a source vertex within 3-hops, it can not be solved by already introduced steps.

Therefore, we would like to introduce path expansion to solve the two problems, which extends the expansion steps `out()`, `in()` and `both()` as the [syntactic sugar](./supported_gremlin_steps.md).

In the three expansion steps, you can now write as `out/in/both(lower..upper, label1, label2...)`, where `lower` is the minimum number of hops in the path, and `upper-1` is the maximum number of hops in the path. For example, `out(3..5)` means expansion through the outgoing edges 3 or 4 times. Therefore,  `lower <= upper - 1`, otherwise the sentence is illegal.

Path expansion is very flexible. Therefore, we design the `with()` step followed by path expansion step to provide two options for user to configure the corresponding behaviors of the path expansion step:

- `PATH_OPT`: Since path expansion allows expand the same step multiple times, it is likely to have duplicate vertices in the path. If the `PATH_OPT` is set to `ARBITRARY`, paths having duplicate vertices will be kept in the traversers; otherwise, if the `PATH_OPT` is set to `SIMPLE_PATH`, paths having duplicate vertices will be filtered.
- `RESULT_OPT`: Sometimes, you may be interested in all the vertices in the path, but sometimes you may be only interested in the last vertex of the path. Therefore, when the `RESULT_OPT` is set to `ALL_V`, all the vertices in the path will be kept to the output. Instead, when the `RESULT_OPT` is set to `END_V`, only the last vertex of the path will be kept.

At last, we would like to introduce `endV()` step in path expansion. Since a path may contain many vertices, all kept vertices are stored in a path collection by default. Therefore, `endV()` is used to unfold the path collection. For example, if the traversers after path expansion is `[[v[1], v[2], v[3]]`, where the inner `[v[1], v[2], v[3]]` is the path collection.   With the `endV()`, `v[1]`, `v[2]` and `v[3]` are picked out from the path collection, and the traversers becomes `[v[1], v[2], v[3]]`

:::{figure-md}
<img src="../images/path_expansion_explanation.png"
     alt="path_expansion_explanation"
     width="70%">

Figure 18. The examples of path expansion.
:::
