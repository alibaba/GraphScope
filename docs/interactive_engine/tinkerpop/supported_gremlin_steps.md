# Supported Gremlin Steps
1. [Introduction](#introduction)
2. [Standard Steps](#standard-steps)
   1. [Source](#source)
   2. [Expand](#expand)
   3. [Filter](#filter)
   4. [Project](#project)
   5. [Aggregate](#aggregate)
   6. [Order](#gremlin-order)
   7. [Statistics](#statistics)
   8. [Union](#union)
   9. [Match](#match)
   10. [Subgraph](#subgraph)
   11. [Identity](#identity)
   12. [Unfold](#unfold)
3. [Syntactic Sugars](#syntactic-sugars)
   1. [PathExpand](#pathexpand)
   2. [Expression](#expression)
   3. [Aggregate(Group)](#aggregate-group)
4. [Limitations](#limitations)
## Introduction
This documentation guides you how to work with the [Gremlin](https://tinkerpop.apache.org/docs/current/reference) graph traversal language in GraphScope. On the one hand we retain the original syntax of most steps from the standard Gremlin, on the other hand the usages of some steps are further extended to denote more complex situations in real-world scenarios.
## Standard Steps
We retain the original syntax of the following steps from the standard Gremlin.
### Source
#### [V()](https://tinkerpop.apache.org/docs/current/reference/#v-step)
The V()-step is meant to iterate over all vertices from the graph. Moreover, `vertexIds` can be injected into the traversal to select a subset of vertices.

Parameters: </br>
vertexIds - to select a subset of vertices from the graph, each id is of integer type.
```bash
g.V()
g.V(1)
g.V(1,2,3)
```
#### [E()](https://tinkerpop.apache.org/docs/current/reference/#e-step)
The E()-step is meant to iterate over all edges from the graph. Moreover, `edgeIds` can be injected into the traversal to select a subset of edges.

Parameters: </br>
edgeIds - to select a subset of edges from the graph, each id is of integer type.
```bash
g.E()
g.E(1)
g.E(1,2,3)
```
### Expand
#### [outE()](https://tinkerpop.apache.org/docs/current/reference/#vertex-steps)
Map the vertex to its outgoing incident edges given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```bash
g.V().outE("knows")
g.V().outE("knows", "created")
```
#### inE()
Map the vertex to its incoming incident edges given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```bash
g.V().inE("knows")
g.V().inE("knows", "created")
```
#### bothE()
Map the vertex to its incident edges given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```bash
g.V().bothE("knows")
g.V().bothE("knows", "created")
```
#### out()
Map the vertex to its outgoing adjacent vertices given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```bash
g.V().out("knows")
g.V().out("knows", "created")
```
#### in()
Map the vertex to its incoming adjacent vertices given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```bash
g.V().in("knows")
g.V().in("knows", "created")
```
#### both()
Map the vertex to its adjacent vertices given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```bash
g.V().both("knows")
g.V().both("knows", "created")
```
#### outV()
Map the edge to its outgoing/tail incident vertex.
```bash
g.V().inE().outV() # = g.V().in()
```
#### inV()
Map the edge to its incoming/head incident vertex.
```bash
g.V().outE().inV() # = g.V().out()
```
#### otherV()
Map the edge to the incident vertex that was not just traversed from in the path history.
```bash
g.V().bothE().otherV() # = g.V().both()
```
#### bothV()
Map the edge to its incident vertices.
```bash
g.V().outE().bothV() # both endpoints of the outgoing edges
```
### Filter
#### [hasId()](https://tinkerpop.apache.org/docs/current/reference/#has-step)
The hasId()-step is meant to filter graph elements based on their identifiers.

Parameters: </br>
elementIds - identifiers of the elements.
```bash
g.V().hasId(1) # = g.V(1)
g.V().hasId(1,2,3) # = g.V(1,2,3)
```
#### hasLabel()
The hasLabel()-step is meant to filter graph elements based on their labels.

Parameters: </br>
labels - labels of the elements.
```bash
g.V().hasLabel("person")
g.V().hasLabel("person", "software")
```
#### has()
The has()-step is meant to filter graph elements by applying predicates on their properties.

Parameters: </br>
* propertyKey - the key of the property to filter on for existence.
    ```bash
    g.V().has("name") # find vertices containing property `name`
    ```
* propertyKey - the key of the property to filter on, </br> value - the value to compare the accessor value to for equality.
    ```bash
    g.V().has("age", 10)
    g.V().has("name", "marko")
    g.E().has("weight", 1.0)
    ```
* propertyKey - the key of the property to filter on, </br> predicate - the filter to apply to the key's value.
    ```bash
    g.V().has("age", P.eq(10))
    g.V().has("age", P.neq(10))
    g.V().has("age", P.gt(10))
    g.V().has("age", P.lt(10))
    g.V().has("age", P.gte(10))
    g.V().has("age", P.lte(10))
    g.V().has("age", P.within([10, 20]))
    g.V().has("age", P.without([10, 20]))
    g.V().has("age", P.inside(10, 20))
    g.V().has("age", P.outside(10, 20))
    g.V().has("age", P.not(P.eq(10))) # = g.V().has("age", P.neq(10))
    g.V().has("name", TextP.startingWith("mar"))
    g.V().has("name", TextP.endingWith("rko"))
    g.V().has("name", TextP.containing("ark"))
    g.V().has("name", TextP.notStartingWith("mar"))
    g.V().has("name", TextP.notEndingWith("rko"))
    g.V().has("name", TextP.notContaining("ark"))
    ```
* label - the label of the Element, </br> propertyKey - the key of the property to filter on, </br> value - the value to compare the accessor value to for equality.
    ```bash
    g.V().has("person", "id", 1) # = g.V().hasLabel("person").has("id", 1)
    ```
* label - the label of the Element, </br> propertyKey - the key of the property to filter on, </br> predicate - the filter to apply to the key's value.
    ```bash
    g.V().has("person", "age", P.eq(10)) # = g.V().hasLabel("person").has("age", P.eq(10))
    ```
#### hasNot()
The hasNot()-step is meant to filter graph elements based on the non-existence of properties.

Parameters: </br>
propertyKey - the key of the property to filter on for non-existence.
```bash
g.V().hasNot("age") # find vertices not-containing property `age`
```
#### [is()](https://tinkerpop.apache.org/docs/current/reference/#is-step)
The is()-step is meant to filter the object if it is unequal to the provided value or fails the provided predicate.

Parameters: </br>
* value - the value that the object must equal.
    ```bash
    g.V().out().count().is(1)
    ```
* predicate - the filter to apply.
    ```bash
    g.V().out().count().is(P.eq(1))
    ```
#### [where(traversal)](https://tinkerpop.apache.org/docs/current/reference/#where-step)
The where(traversal)-step is meant to filter the current object by applying it to the nested traversal.

Parameters: </br>
whereTraversal - the traversal to apply.
```bash
g.V().where(out().count())
g.V().where(out().count().is(gt(0)))
```
#### where(predicate)
The where(predicate)-step is meant to filter the traverser based on the predicate acting on different tags.

Parameters: </br>
* predicate - the predicate containing another tag to apply.
    ```bash
    # is the current entry equal to the entry referred by `a`?
    g.V().as("a").out().out().where(P.eq("a"))
    ```
* startKey - the tag containing the object to filter, </br> predicate - the predicate containing another tag to apply.
    ```bash
    # is the entry referred by `b` equal to the entry referred by `a`?
    g.V().as("a").out().out().as("b").where("b", P.eq("a"))
    ```
The by() can be applied to a number of different steps to alter their behaviors. Here are some usages of the modulated by()-step after a where-step:
* empty - this form is essentially an identity() modulation.
    ```bash
     # = g.V().as("a").out().out().as("b").where("b", P.eq("a"))
    g.V().as("a").out().out().as("b").where("b", P.eq("a")).by()
    ```
* propertyKey - filter by the property value of the specified tag given the property key.
    ```bash
    # whether entry `b` and entry `a` have the same property value of `name`?
    g.V().as("a").out().out().as("b").where("b", P.eq("a")).by("name")
    ```
* traversal - filter by the computed value after applying the specified tag to the nested traversal.
    ```bash
    # whether entry `b` and entry `a` have the same count of one-hop neighbors?
    g.V().as("a").out().out().as("b").where("b", P.eq("a")).by(out().count())
    ```
#### [not(traversal)](https://tinkerpop.apache.org/docs/current/reference/#not-step)
The not()-step is opposite to the where()-step and removes objects from the traversal stream when the traversal provided as an argument does not return any objects.

Parameters: </br>
notTraversal - the traversal to filter by.
```bash
g.V().not(out().count())
g.V().not(out().count().is(gt(0)))
```
#### [dedup()](https://tinkerpop.apache.org/docs/current/reference/#dedup-step)
Remove all duplicates in the traversal stream up to this point.

Parameters:
dedupLabels - composition of the given labels determines de-duplication. No labels implies current object.
```bash
g.V().dedup()
g.V().as("a").out().dedup("a") # dedup by entry `a`
g.V().as("a").out().as("b").dedup("a", "b") # dedup by the composition of entry `a` and `b`
```

Usages of the modulated by()-step: </br>
* propertyKey - dedup by the property value of the current object or the specified tag given the property key.
    ```bash
    # dedup by the property value of `name` of the current entry
    g.V().dedup().by("name")
    # dedup by the property value of `name` of the entry `a`
    g.V().as("a").out().dedup("a").by("name")
    ```
* token - dedup by the token value of the current object or the specified tag.
    ```bash
    g.V().dedup().by(T.id)
    g.V().dedup().by(T.label)
    g.V().as("a").out().dedup("a").by(T.id)
    g.V().as("a").out().dedup("a").by(T.label)
    ```
* traversal - dedup by the computed value after applying the current object or the specified tag to the nested traversal.
    ```bash
    g.V().dedup().by(out().count())
    g.V().as("a").out().dedup("a").by(out().count())
    ```
### Project
#### [id()](https://tinkerpop.apache.org/docs/current/reference/#id-step)
The id()-step is meant to map the graph element to its identifier.
```bash
g.V().id()
```
#### [label()](https://tinkerpop.apache.org/docs/current/reference/#label-step)
The label()-step is meant to map the graph element to its label.
```bash
g.V().label()
```
#### [constant()](https://tinkerpop.apache.org/docs/current/reference/#constant-step)
The constant()-step is meant to map any object to a fixed object value.

Parameters: </br>
value - a fixed object value.
```bash
g.V().constant(1)
g.V().constant("marko")
g.V().constant(1.0)
```

#### [valueMap()](https://tinkerpop.apache.org/docs/current/reference/#valuemap-step)
The valueMap()-step is meant to map the graph element to a map of the property entries according to their actual properties. If no property keys are provided, then all property values are retrieved.

Parameters: </br>
propertyKeys - the properties to retrieve.
```bash
g.V().valueMap()
g.V().valueMap("name")
g.V().valueMap("name", "age")
```

#### [values()](https://tinkerpop.apache.org/docs/current/reference/#values-step)
The values()-step is meant to map the graph element to the values of the associated properties given the provide property keys. Here we just allow only one property key as the argument to the `values()` to implement the step as a map instead of a flat-map, which may be a little different from the standard Gremlin.

Parameters: </br>
propertyKey - the property to retrieve its value from.
```bash
g.V().values("name")
```

#### [elementMap()](https://tinkerpop.apache.org/docs/current/reference/#elementmap-step)
The elementMap()-step is meant to map the graph element to a map of T.id, T.label and the property values according to the given keys. If no property keys are provided, then all property values are retrieved.
```

Parameters: </br>
propertyKeys - the properties to retrieve.
```bash
g.V().elementMap()
g.V().elementMap("name")
g.V().elementMap("name", "age")
```

#### [select()](https://tinkerpop.apache.org/docs/current/reference/#select-step)
The select()-step is meant to map the traverser to the object specified by the selectKey or to a map projection of sideEffect values.

Parameters: </br>
selectKeys - the keys to project.
```bash
g.V().as("a").select("a")
g.V().as("a").out().as("b").select("a", "b")
```

Usages of the modulated by()-step: </br>
* empty - an identity() modulation.
    ```bash
    # = g.V().as("a").select("a")
    g.V().as("a").select("a").by()
    # = g.V().as("a").out().as("b").select("a", "b")
    g.V().as("a").out().as("b").select("a", "b").by().by()
    ```
* token - project the token value of the specified tag.
    ```bash
    g.V().as("a").select("a").by(T.id)
    g.V().as("a").select("a").by(T.label)
    ```
* propertyKey - project the property value of the specified tag given the property key.
    ```bash
    g.V().as("a").select("a").by("name")
    ```
* traversal - project the computed value after applying the specified tag to the nested traversal.
    ```bash
    g.V().as("a").select("a").by(valueMap("name", "id"))
    g.V().as("a").select("a").by(out().count())
    ```
### Aggregate
#### [count()](https://tinkerpop.apache.org/docs/current/reference/#count-step)
Count the number of traverser(s) up to this point.
```bash
g.V().count()
```
#### [fold()](https://tinkerpop.apache.org/docs/current/reference/#fold-step)
Rolls up objects in the stream into an aggregate list.
```bash
# select top-10 vertices from the stream and fold them into single list
g.V().limit(10).fold()
```
#### [sum()](https://tinkerpop.apache.org/docs/current/reference/#sum-step)
Sum the traverser values up to this point.
```bash
g.V().values("age").sum()
```
#### [min()](https://tinkerpop.apache.org/docs/current/reference/#min-step)
Determines the minimum value in the stream.
```bash
g.V().values("age").min()
```
#### [max()](https://tinkerpop.apache.org/docs/current/reference/#max-step)
Determines the maximum value in the stream.
```bash
g.V().values("age").max()
```
#### [mean()](https://tinkerpop.apache.org/docs/current/reference/#mean-step)
Compute the average value in the stream.
```bash
g.V().values("age").mean()
```
#### [group()](https://tinkerpop.apache.org/docs/current/reference/#group-step)
Organize objects in the stream into a Map. Calls to group() are typically accompanied with by() modulators which help specify how the grouping should occur.

Usages of the key by()-step:
* empty - group the elements in the stream by the current value.
    ```bash
    g.V().group().by() # = g.V().group()
    ```
* propertyKey - group the elements in the stream by the property value of the current object given the property key.
    ```bash
    g.V().group().by("name")
    ```
* traversal - group the elements in the stream by the computed value after applying the current object to the nested traversal.
    ```bash
    g.V().group().by(values("name")) # = g.V().group().by("name")
    g.V().group().by(out().count())
    ```

Usages of the value by()-step:
* empty - fold elements in each group into a list, which is a default behavior.
    ```bash
    g.V().group().by().by() # = g.V().group()
    ```
* propertyKey - for each element in the group, get their property values according to the given keys.
    ```bash
    g.V().group().by().by("name")
    ```
* aggregateFunc - aggregate function to apply in each group.
    ```bash
    g.V().group().by().by(count())
    g.V().group().by().by(fold())
    # get the property values of `name` of the vertices in each group list
    g.V().group().by().by(values("name").fold()) # = g.V().group().by().by("name")
    # sum the property values of `age` in each group
    g.V().group().by().by(values("age").sum())
    # find the minimum value of `age` in each group
    g.V().group().by().by(values("age").min())
    # find the maximum value of `age` in each group
    g.V().group().by().by(values("age").max())
    # calculate the average value of `age` in each group
    g.V().group().by().by(values("age").mean())
    # count the number of distinct elements in each group
    g.V().group().by().by(dedup().count())
    # de-duplicate in each group list
    g.V().group().by().by(dedup().fold())
    ```
#### [groupCount()](https://tinkerpop.apache.org/docs/current/reference/#groupcount-step)
Counts the number of times a particular objects has been part of a traversal, returning a map where the object is the key and the value is the count.

Usages of the key by()-step:
* empty - group the elements in the stream by the current value.
    ```bash
    g.V().groupCount().by() # = g.V().groupCount()
    ```
* propertyKey - group the elements in the stream by the property value of the current object given the property key.
    ```bash
    g.V().groupCount().by("name")
    ```
* traversal - group the elements in the stream by the computed value after applying the current object to the nested traversal.
    ```bash
    g.V().groupCount().by(values("name")) # = g.V().groupCount().by("name")
    g.V().groupCount().by(out().count())
    ```
### <h3 id="gremlin-order">Order</h3>
#### [order()](https://tinkerpop.apache.org/docs/current/reference/#order-step)
Order all the objects in the traversal up to this point and then emit them one-by-one in their ordered sequence.

Usages of the modulated by()-step: </br>
* empty - order by the current object in ascending order, which is a default behavior.
    ```bash
    g.V().order().by() # = g.V().order()
    ```
* order - the comparator to apply typically for some order (asc | desc | shuffle).
    ```bash
    g.V().order().by(Order.asc) # = g.V().order()
    g.V().order().by(Order.desc)
    ```
* propertyKey - order by the property value of the current object given the property key.
    ```bash
    g.V().order().by("name") # default order is asc
    g.V().order().by("age")
    ```
* traversal - order by the computed value after applying the current object to the nested traversal.
    ```bash
    g.V().order().by(out().count()) # default order is asc
    ```
* propertyKey - order by the property value of the current object given the property key, </br> order - the comparator to apply typically for some order.
    ```bash
    g.V().order().by("name", Order.desc)
    ```
* traversal - order by the computed value after applying the current object to the nested traversal, </br> order - the comparator to apply typically for some order.
    ```bash
    g.V().order().by(out().count(), Order.desc)
    ```
### Statistics
#### [limit()](https://tinkerpop.apache.org/docs/current/reference/#limit-step)
Filter the objects in the traversal by the number of them to pass through the stream, where only the first n objects are allowed as defined by the limit argument.

Parameters: </br>
limit - the number at which to end the stream.
```bash
g.V().limit(10)
```
#### [coin()](https://tinkerpop.apache.org/docs/current/reference/#coin-step)
Filter the object in the stream given a biased coin toss.

Parameters: </br>
probability - the probability that the object will pass through.
```bash
g.V().coin(0.2) # range is [0.0, 1.0]
g.V().out().coin(0.2)
```

#### [sample()](https://tinkerpop.apache.org/docs/current/reference/#sample-step)
Generate a certain number of sample results.

Parameters: </br>
number - allow specified number of objects to pass through the stream.
```bash
g.V().sample(10)
g.V().out().sample(10)
```

### Union
#### [union()](https://tinkerpop.apache.org/docs/current/reference/#union-step)
Merges the results of an arbitrary number of traversals.

Parameters: </br>
unionTraversals - the traversals to merge.
```bash
g.V().union(out(), out().out())
```
### Match
#### [match()](https://tinkerpop.apache.org/docs/current/reference/#match-step)
The match()-step provides a declarative form of graph patterns to match with. With match(), the user provides a collection of "sentences," called patterns, that have variables defined that must hold true throughout the duration of the match(). For most of the complex graph patterns, it is usually much easier to express via match() than with single-path traversals.

Parameters: </br>
matchSentences - define a collection of patterns. Each pattern consists of a start tag, a serials of Gremlin steps (binders) and an end tag.

Supported binders within a pattern: </br>
* Expand: in()/out()/both(), inE()/outE()/bothE(), inV()/outV()/otherV/bothV
* PathExpand
* Filter: has()/not()/where
```bash
g.V().match(__.as("a").out().as("b"), __.as("b").out().as("c"))
g.V().match(__.as("a").out().out().as("b"), where(__.as("a").out().as("b")))
g.V().match(__.as("a").out().out().as("b"), not(__.as("a").out().as("b")))
g.V().match(__.as("a").out().has("name", "marko").as("b"), __.as("b").out().as("c"))
```
### Subgraph
#### [subgraph()](https://tinkerpop.apache.org/docs/current/reference/#subgraph-step)
An edge-induced subgraph extracted from the original graph.

Parameters: </br>
graphName - the name of the side-effect key that will hold the subgraph.
```bash
g.E().subgraph("all")
g.V().has('name', "marko").outE("knows").subgraph("partial")
```
### Identity
#### [identity()](https://tinkerpop.apache.org/docs/current/reference/#identity-step)
The identity()-step maps the current object to itself.

```bash
g.V().identity().values("id")
g.V().hasLabel("person").as("a").identity().values("id")
g.V().has("name", "marko").union(identity(), out()).values("id")
```
### Unfold
#### [unfold()](https://tinkerpop.apache.org/docs/current/reference/#unfold-step)
The unfold()-step unrolls an iterator, iterable or map into a linear form.
```bash
g.V().fold().unfold().values("id")
g.V().fold().as("a").unfold().values("id")
g.V().has("name", "marko").fold().as("a").select("a").unfold().values("id")
g.V().out("1..3", "knows").with('RESULT_OPT', 'ALL_V').unfold()
```
## Syntactic Sugars
The following steps are extended to denote more complex situations.
### PathExpand
In Graph querying, expanding a multiple-hops path from a starting point is called `PathExpand`, which is commonly used in graph scenarios. In addition, there are different requirements for expanding strategies in different scenarios, i.e. it is required to output a simple path or all vertices explored along the expanding path. We introduce the with()-step to configure the corresponding behaviors of the `PathExpand`-step.

#### out()
Expand a multiple-hops path along the outgoing edges, which length is within the given range.

Parameters: </br>
lengthRange - the lower and the upper bounds of the path length, </br> edgeLabels - the edge labels to traverse.

Usages of the with()-step: </br>
keyValuePair - the options to configure the corresponding behaviors of the `PathExpand`-step.

Below are the supported values and descriptions for `PATH_OPT` and `RESULT_OPT` options.

 Parameter | Supported Values  | Description                                    
-----------|-------------------|------------------------------------------------
 PATH_OPT  | ARBITRARY         | Allow vertices or edges to be duplicated.     
 PATH_OPT  | SIMPLE            | No duplicated nodes.            
 PATH_OPT  | TRAIL             | No duplicated edges.            
 RESULT_OPT| END_V             | Only keep the end vertex.                      
 RESULT_OPT| ALL_V             | Keep all vertices along the path.              
 RESULT_OPT| ALL_V_E           | Keep all vertices and edges along the path.    

```bash
# expand hops within the range of [1, 10) along the outgoing edges,
# vertices can be duplicated and only the end vertex should be kept
g.V().out("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'END_V')
# expand hops within the range of [1, 10) along the outgoing edges,
# vertices and edges can be duplicated, and all vertices and edges along the path should be kept
g.V().out("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'ALL_V_E')
# expand hops within the range of [1, 10) along the outgoing edges,
# edges cannot be duplicated, and all vertices and edges along the path should be kept
g.V().out("1..10").with('PATH_OPT', 'TRAIL').with('RESULT_OPT', 'ALL_V_E')
# expand hops within the range of [1, 10) along the outgoing edges,
# vertices cannot be duplicated and all vertices should be kept
g.V().out("1..10").with('PATH_OPT', 'SIMPLE').with('RESULT_OPT', 'ALL_V')
# = g.V().out("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'END_V')
g.V().out("1..10")
# expand hops within the range of [1, 10) along the outgoing edges which label is `knows`,
# vertices can be duplicated and only the end vertex should be kept
g.V().out("1..10", "knows")
# expand hops within the range of [1, 10) along the outgoing edges which label is `knows` or `created`,
# vertices can be duplicated and only the end vertex should be kept
g.V().out("1..10", "knows", "created")
# expand hops within the range of [1, 10) along the outgoing edges,
# and project the properties "id" and "name" of every vertex along the path
g.V().out("1..10").with('RESULT_OPT', 'ALL_V').values("name")
```
Running Example:
```bash
gremlin> g.V().out("1..3", "knows").with('RESULT_OPT', 'ALL_V')
==>[v[1], v[2]]
==>[v[1], v[4]]
gremlin> g.V().out("1..3", "knows").with('RESULT_OPT', 'ALL_V_E')
==>[v[1], e[0][1-knows->2], v[2]]
==>[v[1], e[2][1-knows->4], v[4]]
gremlin> g.V().out("1..3", "knows").with('RESULT_OPT', 'END_V').endV()
==>v[2]
==>v[4]
gremlin> g.V().out("1..3", "knows").with('RESULT_OPT', 'ALL_V').values("name")
==>[marko, vadas]
==>[marko, josh]
gremlin> g.V().out("1..3", "knows").with('RESULT_OPT', 'ALL_V').valueMap("id","name")
==>{id=[[1, 2]], name=[[marko, vadas]]}
==>{id=[[1, 4]], name=[[marko, josh]]}
```
#### in()
Expand a multiple-hops path along the incoming edges, which length is within the given range.
```bash
g.V().in("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'END_V')
```
Running Example:
```bash
gremlin> g.V().in("1..3", "knows").with('RESULT_OPT', 'ALL_V')
==>[v[2], v[1]]
==>[v[4], v[1]]
gremlin> g.V().in("1..3", "knows").with('RESULT_OPT', 'ALL_V_E')
==>[v[2], e[0][1-knows->2], v[1]]
==>[v[4], e[2][1-knows->4], v[1]]
gremlin> g.V().in("1..3", "knows").with('RESULT_OPT', 'END_V').endV()
==>v[1]
==>v[1]
```
#### both()
Expand a multiple-hops path along the incident edges, which length is within the given range.
```bash
g.V().both("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'END_V')
```
Running Example:
```bash
gremlin> g.V().both("1..3", "knows").with('RESULT_OPT', 'ALL_V')
==>[v[2], v[1]]
==>[v[1], v[2]]
==>[v[1], v[4]]
==>[v[2], v[1], v[2]]
==>[v[2], v[1], v[4]]
==>[v[4], v[1]]
==>[v[1], v[2], v[1]]
==>[v[1], v[4], v[1]]
==>[v[4], v[1], v[2]]
==>[v[4], v[1], v[4]]
gremlin> g.V().both("1..3", "knows").with('RESULT_OPT', 'ALL_V_E')
==>[v[2], e[0][1-knows->2], v[1]]
==>[v[4], e[2][1-knows->4], v[1]]
==>[v[1], e[0][1-knows->2], v[2]]
==>[v[1], e[2][1-knows->4], v[4]]
==>[v[2], e[0][1-knows->2], v[1], e[0][1-knows->2], v[2]]
==>[v[2], e[0][1-knows->2], v[1], e[2][1-knows->4], v[4]]
==>[v[4], e[2][1-knows->4], v[1], e[0][1-knows->2], v[2]]
==>[v[4], e[2][1-knows->4], v[1], e[2][1-knows->4], v[4]]
==>[v[1], e[0][1-knows->2], v[2], e[0][1-knows->2], v[1]]
==>[v[1], e[2][1-knows->4], v[4], e[2][1-knows->4], v[1]]
gremlin> g.V().both("1..3", "knows").with('RESULT_OPT', 'END_V').endV()
==>v[1]
==>v[1]
==>v[2]
==>v[4]
==>v[2]
==>v[1]
==>v[1]
==>v[4]
==>v[2]
==>v[4]
```
#### endV()
By default, all kept vertices are stored in a path collection which can be unfolded by a `endV()`-step.
```bash
# a path collection containing the vertices within [1, 10) hops
g.V().out("1..10").with('RESULT_OPT', 'ALL_V')
# unfold vertices in the path collection
g.V().out("1..10").with('RESULT_OPT', 'ALL_V').endV()
```
#### Getting Properites
The properties of the elements (vertices and/or edges) in the path can be projected by `values()`-step, `valueMap()`-step, and `elementMap()`-step.
It is important to note that the specific elements targeted for property projection are determined by the `RESULT_OPT` setting. 
For instance, if you configure `RESULT_OPT` as `ALL_V`,  `values()`, `valueMap()`, or `elementMap()` will then project the properties of all vertices present in the path. It's important to be aware that:
1. If a property doesn't exist on the current vertex, these methods will return null for that property.
2. By default, valueMap() and elementMap() return all properties on a vertex (or edge). However, within a path, they return a collection of properties from all vertex (or edge) types in the path. If certain vertex (or edge) types lack some of these properties, the methods will again return null for the not existed properties.
```bash
# get properties of each vertex in the path
gremlin> g.V().both("1..3","knows").with('RESULT_OPT', 'ALL_V').values("name")
==>[vadas, marko]
==>[josh, marko]
==>[marko, vadas]
==>[marko, josh]
==>[vadas, marko, vadas]
==>[vadas, marko, josh]
==>[josh, marko, vadas]
==>[josh, marko, josh]
==>[marko, vadas, marko]
==>[marko, josh, marko]
gremlin> g.V().both("1..3","knows").with('RESULT_OPT', 'ALL_V').valueMap("name","age")
==>[{age=27, name=vadas}, {age=29, name=marko}]
==>[{age=32, name=josh}, {age=29, name=marko}]
==>[{age=29, name=marko}, {age=27, name=vadas}, {age=29, name=marko}]
==>[{age=29, name=marko}, {age=32, name=josh}, {age=29, name=marko}]
==>[{age=29, name=marko}, {age=27, name=vadas}]
==>[{age=29, name=marko}, {age=32, name=josh}]
==>[{age=27, name=vadas}, {age=29, name=marko}, {age=27, name=vadas}]
==>[{age=27, name=vadas}, {age=29, name=marko}, {age=32, name=josh}]
==>[{age=32, name=josh}, {age=29, name=marko}, {age=27, name=vadas}]
==>[{age=32, name=josh}, {age=29, name=marko}, {age=32, name=josh}]
gremlin> g.V().hasLabel("person").both("1..2").with('RESULT_OPT', 'ALL_V').elementMap()
==>[{age=29, id=1, lang=null, name=marko, ~id=1, ~label=0}, {age=27, id=2, lang=null, name=vadas, ~id=2, ~label=0}]
==>[{age=29, id=1, lang=null, name=marko, ~id=1, ~label=0}, {age=32, id=4, lang=null, name=josh, ~id=4, ~label=0}]
==>[{age=29, id=1, lang=null, name=marko, ~id=1, ~label=0}, {age=null, id=3, lang=java, name=lop, ~id=72057594037927939, ~label=1}]
==>[{age=27, id=2, lang=null, name=vadas, ~id=2, ~label=0}, {age=29, id=1, lang=null, name=marko, ~id=1, ~label=0}]
==>[{age=32, id=4, lang=null, name=josh, ~id=4, ~label=0}, {age=null, id=3, lang=java, name=lop, ~id=72057594037927939, ~label=1}]
==>[{age=32, id=4, lang=null, name=josh, ~id=4, ~label=0}, {age=null, id=5, lang=java, name=ripple, ~id=72057594037927941, ~label=1}]
==>[{age=32, id=4, lang=null, name=josh, ~id=4, ~label=0}, {age=29, id=1, lang=null, name=marko, ~id=1, ~label=0}]
==>[{age=35, id=6, lang=null, name=peter, ~id=6, ~label=0}, {age=null, id=3, lang=java, name=lop, ~id=72057594037927939, ~label=1}]
```

### Expression

Expressions, expressed via the `expr()` syntactic sugar, have been introduced to facilitate writing expressions directly within steps such as `select()`, `project()`, `where()`, and `group()`. This update is part of an ongoing effort to standardize Gremlin's expression syntax, making it more aligned with [SQL expression syntax](https://www.w3schools.com/sql/sql_operators.asp). The updated syntax, effective from version 0.27.0, streamlines user operations and enhances readability. Below, we detail the updated syntax definitions and point out key distinctions from the syntax used prior to version 0.26.0.

Literal:

Category | Syntax
---- | -------
string | "marko"
boolean | true, false
integer | 1, 2, 3
long | 1l, 1L
float | 1.0f, 1.0F
double | 1.0, 1.0d, 1.0D
list | ["marko", "vadas"], [true, false], [1, 2], [1L, 2L], [1.0F, 2.0F], [1.0, 2.0]

Variable:

Category | Description | Before 0.26.0 | Since 0.27.0
---- | ------- | ------- | -------
current | the current entry | @ | _
current property | the property value of the current entry | @.name | _.name
tag | the specified tag | @a | a
tag property | the property value of the specified tag | @a.name | a.name

Operator:

Category | Operation (Case-Insensitive) | Description | Before 0.26.0  | Since 0.27.0
---- | ------- | ------- | ------- | -------
logical | = | equal | @.name == "marko" | _.name = "marko"
logical | <> | not equal | @.name != "marko" | _.name != "marko"
logical | > | greater than | @.age > 10 | _.age > 10
logical | < | less than | @.age < 10 | _.age < 10
logical | >= | greater than or equal | @.age >= 10 | _.age >= 10
logical | <= | less than or equal | @.age <= 10 | _.age <= 10
logical | NOT | negate the logical expression | ! (@.name == "marko") | NOT _.name = "marko"
logical | AND | connect two logical expressions with AND | @.name == "marko" && @.age > 10 | _.name = "marko" AND _.age > 10
logical | OR | connect two logical expressions with OR | @.name == "marko" \|\| @.age > 10 | _.name = "marko" OR _.age > 10
logical | IN | whether the value of the current entry is in the given list | @.name WITHIN ["marko", "vadas"] | _.name IN ["marko", "vadas"]
logical | IS NULL | whether the value of the current entry ISNULL | @.age IS NULL | _.age IS NULL
logical | IS NOT NULL | whether the value of the current entry IS NOT NULL | ! (@.age ISNULL) | _.age IS NOT NULL
arithmetical | + | addition | @.age + 10 | _.age + 10
arithmetical | - | subtraction | @.age - 10 | _.age - 10
arithmetical | * | multiplication | @.age * 10 | _.age * 10
arithmetical | / | division | @.age / 10 | _.age / 10
arithmetical | % | modulo | @.age % 10 | _.age % 10
arithmetical | POWER | exponentiation | @.age ^^ 3 | POWER(_.age, 3)
temporal arithmetical | + | Add a duration to a temporal type | unsupported | _.creationDate + duration({years: 1})
temporal arithmetical | - | Subtract a duration from a temporal type | unsupported | _.creationDate - duration({years: 1})
temporal arithmetical | - | Subtract two temporal types, returning a duration in milliseconds | unsupported | a.creationDate - b.creationDate
temporal arithmetical | + | Add two durations | unsupported | duration({years: 1}) + duration({months: 2})
temporal arithmetical | - | Subtract two durations | unsupported | duration({years: 1}) - duration({months: 2})
temporal arithmetical | * | Multiply a duration by a numeric value | unsupported | duration({years: 1}) * 2
temporal arithmetical | / | Divide a duration by a numeric value | unsupported | duration({years: 1}) / 2, (a.creationDate - b.creationDate) / 1000
bitwise | & | bitwise AND | @.age & 2 | _.age & 2
bitwise | \| | bitwise OR | @.age \| 2 | _.age \| 2
bitwise | ^ | bitwise XOR | @.age ^ 2 | _.age ^ 2
bit shift | << | left shift | @.age << 2 | _.age << 2
bit shift | >> | right shift | @.age >> 2 | _.age >> 2
string regex match | STARTS WITH | whether the string starts with the given prefix | @.name STARTSWITH "ma" | _.name STARTS WITH "ma"
string regex match | NOT STARTS WITH | whether the string does not start with the given prefix | ! (@.name STARTSWITH "ma") | NOT _.name STARTS WITH "ma"
string regex match | ENDS WITH | whether the string ends with the given suffix | @.name ENDSWITH "ko" | _.name ENDS WITH "ko"
string regex match | NOT ENDS WITH | whether the string does not end with the given suffix | ! (@.name ENDSWITH "ko") | NOT _.name ENDS WITH "ko"
string regex match | CONTAINS | whether the string contains the given substring | "ar" WITHIN @.name | _.name CONTAINS "ar"
string regex match | NOT CONTAINS | whether the string does not contain the given substring | "ar" WITHOUT @.name | NOT _.name CONTAINS "ar"

Function:

Category | Function (Case-Insensitive) | Description | Before 0.26.0 | Since 0.27.0
---- | ------- | ------- | ------- | -------
aggregate | COUNT | count the number of the elements | unsupported | COUNT(_.age)
aggregate | SUM | sum the values of the elements | unsupported | SUM(_.age)
aggregate | MIN | find the minimum value of the elements | unsupported | MIN(_.age)
aggregate | MAX | find the maximum value of the elements | unsupported | MAX(_.age)
aggregate | AVG | calculate the average value of the elements | unsupported | AVG(_.age)
aggregate | COLLECT | fold the elements into a list | unsupported | COLLECT(_.age)
aggregate | HEAD(COLLECT()) | find the first value of the elements | unsupported | HEAD(COLLECT(_.age))
other | LABELS | get the labels of the specified tag which is a vertex | @a.~label | LABELS(a)
other | elementId | get a vertex or an edge identifier, unique by an object type and a database | @a.~id | elementId(a)
other | TYPE | get the type of the specified tag which is an edge | @a.~label |TYPE(a)
other | LENGTH | get the length of the specified tag which is a path | @a.~len | LENGTH(a)

Expression in project or filter:
Category | Description | Before 0.26.0 | Since 0.27.0
---- | ------- | ------- | -------
filter | filter the current traverser by the expression | where(expr("@.name == \\"marko\\"")) | where(expr(_.name = "marko"))
project | project the current traverser to the value of the expression | select(expr("@.name")) | select(expr(_.name))

Here we provide the precedence of the operators mentioned above, which is also based on the SQL standard.

Precedence | Operator | Description | Associativity
--- | --- | --- | ---
1 | `()`, `.`, power(), count()... | Parentheses, Member access, Function call | Left-to-right
2 | -a, +a | Unary minus, Unary plus | Right-to-left
3 | `*`, `/`, `%` | Multiplication, Division, Modulus | Left-to-right
4 | `+`, `-`, `&`, `\|`, `^`, `<<`, `>>` | Addition, Subtraction, Bitwise AND, Bitwise OR, Bitwise XOR, Left shift, Right shift | Left-to-right
5 | STARTS WITH, ENDS WITH, CONTAINS, IN | String regex match, Collection membership | Left-to-right
6 | `=`, `<>`, `<`, `<=`, `>`, `>=` | Comparison | Left-to-right
7 | IS NULL, IS NOT NULL | Nullness check | Left-to-right
8 | NOT | Logical NOT | Right-to-left
9 | AND | Logical AND | Left-to-right
10 | OR | Logical OR | Left-to-right

#### Running Examples

```bash
gremlin> :submit g.V().where(expr(_.name = "marko"))
==>v[1]
gremlin> :submit g.V().as("a").where(expr(a.name = "marko" OR a.age > 10))
==>v[6]
==>v[1]
==>v[2]
==>v[4]
gremlin> :submit g.V().as("a").where(expr(a.age IS NULL)).values("name")
==>lop
==>ripple
gremlin> :submit g.V().as("a").where(expr(a.age IS NOT NULL)).values("name")
==>vadas
==>josh
==>marko
==>peter
gremlin> :submit g.V().as("a").where(expr(a.name STARTS WITH "ma"))
==>v[1]
gremlin> :submit g.V().select(expr(_.name))
==>vadas
==>josh
==>lop
==>ripple
==>marko
==>peter
gremlin> :submit g.V().hasLabel("person").select(expr(_.age ^ 1))
==>26
==>28
==>33
==>34
gremlin> :submit g.V().hasLabel("person").select(expr(POWER(_.age, 2)))
==>729
==>1024
==>1225
==>841
```

### Aggregate (Group)
The group()-step in standard Gremlin has limited capabilities (i.e. grouping can only be performed based on a single key, and only one aggregate calculation can be applied in each group), which cannot be applied to the requirements of performing group calculations on multiple keys or values; Therefore, we further extend the capabilities of the group()-step, allowing multiple variables to be set and different aliases to be configured in key by()-step and value by()-step respectively.

Usages of the key by()-step:
```bash
# group by the property values of `name` and `age` of the current entry
group().by(values("name").as("k1"), values("age").as("k2"))
# group by the count of one-hop neighbors and the property value of `age` of the current entry
group().by(out().count().as("k1"), values("name").as("k2"))
```

Usages of the value by()-step:
```bash
# calculate the count of vertices and the sum of `age` respectively in each group
group().by("name").by(count().as("v1"), values("age").sum().as("v2"))
```
Running Example:
```bash
gremlin> g.V().hasLabel("person").group().by(values("name").as("k1"), values("age").as("k2"))
==>{[josh, 32]=[v[4]], [vadas, 27]=[v[2]], [peter, 35]=[v[6]], [marko, 29]=[v[1]]}
gremlin> g.V().hasLabel("person").group().by(out().count().as("k1"), values("name").as("k2"))
==>{[2, josh]=[v[4]], [0, vadas]=[v[2]], [3, marko]=[v[1]], [1, peter]=[v[6]]}
gremlin> g.V().hasLabel("person").group().by("name").by(count().as("v1"), values("age").sum().as("v2"))
==>{marko=[1, 29], peter=[1, 35], josh=[1, 32], vadas=[1, 27]}
gremlin> g.V().hasLabel("person").group().by("name").by(count().as("v1"), values("age").sum().as("v2")).select("v1", "v2")
==>{v1=1, v2=35}
==>{v1=1, v2=32}
==>{v1=1, v2=27}
==>{v1=1, v2=29}
```
## Limitations
Here we list steps which are unsupported yet. Some will be supported in the near future while others will remain unsupported for some reasons.

### To be Supported
The following steps will be supported in the near future.
<!--#### identity()
Map the current object to itself.
```bash
g.V().identity()
g.V().union(identity(), out().out())
```<-->
#### path()
Map the traverser to its path history.
```bash
g.V().out().out().path()
g.V().as("a").out().out().select("a").by("name").path()
```
#### unfold()
Unrolls a iterator, iterable or map into a linear form.
```bash
g.V().fold().unfold()
```
#### local()
```bash
g.V().fold().count(local)
g.V().values('age').fold().sum(local)
```
### Will Not be Supported
The following steps will remain unsupported.
#### repeat()
* repeat().times() </br>
In graph pattern scenarios, `repeat().times()` can be replaced equivalently by the `PathExpand`-step.
    ```bash
    # = g.V().out("2..3", "knows").endV()
    g.V().repeat(out("knows")).times(2)

    # = g.V().out("1..3", "knows").endV()
    g.V().repeat(out("knows")).emit().times(2)

    # = g.V().out("2..3", "knows").with('PATH_OPT', 'SIMPLE').endV()
    g.V().repeat(out("knows").simplePath()).times(2)

    # = g.V().out("1..3", "knows").with('PATH_OPT', 'SIMPLE').endV()
    g.V().repeat(out("knows").simplePath()).emit().times(2)
    ```
* repeat().until() </br>
It is a imperative syntax, not declarative.
#### properties()
The properties()-step retrieves and then unfolds properties from a graph element. The valueMap()-step can reflect all the properties of each graph element in a map form, which could be much more clear than the results of the properties()-step for the latter could mix up the properties of all the graph elements in the same output.
#### sideEffect
It is required to maintain global variables for `SideEffect`-step during actual execution, which is hard to implement in distributed scenarios. i.e.
* group("a")
* groupCount("a")
* aggregate("a")
* sack()
#### branch
Currently, we only support the operations of merging multiple streams into one. The following splitting operations are unsupported:
* branch()
* choose()
