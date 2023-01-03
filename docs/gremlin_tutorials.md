# Tutorials for Gremlin Users

## Introduction
This documentation guides you how to work with the gremlin graph traversal language in GraphScope. On the one hand we retain the original syntax of most steps from the standard gremlin, on the other hand the usages of some steps are further extended to denote more complex situations in real-world scenarios.

## Steps
### Source
#### V()
The V()-step is meant to iterate over all vertices from the graph. Moreover, `vertexIds` can be injected into the traversal to select a subset of vertices.

Parameters: </br>
vertexIds - to select a subset of vertices from the graph, each id is of integer type.
```java
g.V()
g.V(1)
g.V(1,2,3)
```
#### E()
The E()-step is meant to iterate over all edges from the graph. Moreover, `edgeIds` can be injected into the traversal to select a subset of edges.

Parameters: </br>
edgeIds - to select a subset of edges from the graph, each id is of integer type.
```java
g.E()
g.E(1)
g.E(1,2,3)
```
### Expand
#### outE()
Map the vertex to its outgoing incident edges given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```java
g.V().outE("knows")
g.V().outE("knows", "created")
```
#### inE()
Map the vertex to its incoming incident edges given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```java
g.V().inE("knows")
g.V().inE("knows", "created")
```
#### bothE()
Map the vertex to its incident edges given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```java
g.V().bothE("knows")
g.V().bothE("knows", "created")
```
#### out()
Map the vertex to its outgoing adjacent vertices given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```java
g.V().out("knows")
g.V().out("knows", "created")
```
#### in()
Map the vertex to its incoming adjacent vertices given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```java
g.V().in("knows")
g.V().in("knows", "created")
```
#### both()
Map the vertex to its adjacent vertices given the edge labels.

Parameters: </br>
edgeLabels - the edge labels to traverse.
```java
g.V().both("knows")
g.V().both("knows", "created")
```
#### outV()
Map the edge to its outgoing/tail incident vertex.
```java
g.V().inE().outV() // = g.V().in()
```
#### inV()
Map the edge to its incoming/head incident vertex.
```java
g.V().outE().inV() // = g.V().out()
```
#### otherV()
Map the edge to the incident vertex that was not just traversed from in the path history.
```java
g.V().bothE().otherV() // = g.V().both()
```
#### bothV()
Map the edge to its incident vertices.
```java
g.V().outE().bothV() // both endpoints of the outgoing edges
```
### Filter
#### hasId()
The hasId()-step is meant to filter graph elements based on their identifiers.

Parameters: </br>
elementIds - identifiers of the elements. 
```java
g.V().hasId(1) // = g.V(1)
g.V().hasId(1,2,3) // = g.V(1,2,3)
```
#### hasLabel()
The hasLabel()-step is meant to filter graph elements based on their labels.

Parameters: </br>
labels - labels of the elements. 
```java
g.V().hasLabel("person")
g.V().hasLabel("person", "software")
```
#### has()
The has()-step is meant to filter graph elements by applying predicates on their properties.

Parameters: </br>
* propertyKey - the key of the property to filter on for existence. 
    ```java
    g.V().has("name") // find vertices containing property `name`
    ```
* propertyKey - the key of the property to filter on, </br> value - the value to compare the accessor value to for equality. 
    ```java
    g.V().has("age", 10)
    g.V().has("name", "marko")
    g.E().has("weight", 1.0)    
    ```
* propertyKey - the key of the property to filter on, </br> predicate - the filter to apply to the key's value. 
    ```java
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
    g.V().has("age", P.not(P.eq(10))) // = g.V().has("age", P.neq(10))
    g.V().has("name", TextP.startingWith("mar"))
    g.V().has("name", TextP.endingWith("rko"))
    g.V().has("name", TextP.containing("ark"))
    g.V().has("name", TextP.notStartingWith("mar"))
    g.V().has("name", TextP.notEndingWith("rko"))
    g.V().has("name", TextP.notContaining("ark"))
    ```
* label - the label of the Element, </br> propertyKey - the key of the property to filter on, </br> value - the value to compare the accessor value to for equality. 
    ```java
    g.V().has("person", "id", 1) // = g.V().hasLabel("person").has("id", 1)
    ```
* label - the label of the Element, </br> propertyKey - the key of the property to filter on, </br> predicate - the filter to apply to the key's value. 
    ```java
    g.V().has("person", "age", P.eq(10)) // = g.V().hasLabel("person").has("age", P.eq(10))
    ```
#### hasNot()
The hasNot()-step is meant to filter graph elements based on the non-existence of properties.

Parameters: </br>
propertyKey - the key of the property to filter on for non-existence.
```java
g.V().hasNot("age") // find vertices not-containing property `age`
```
#### is()
The is()-step is meant to filter the object if it is unequal to the provided value or fails the provided predicate.

Parameters: </br>
* value - the value that the object must equal.
    ```java
    g.V().out().count().is(1)
    ```
* predicate - the filter to apply.
    ```java
    g.V().out().count().is(P.eq(1))
    ```
#### where(traversal)
The where(traversal)-step is meant to filter the current object by applying it to the nested traversal.

Parameters: </br>
whereTraversal - the traversal to apply.
```java
g.V().where(out().count())
g.V().where(out().count().is(gt(0)))
```
#### where(predicate)
The where(predicate)-step is meant to filter the traverser based on the predicate acting on different tags.

Parameters: </br>
* predicate - the predicate containing another tag to apply.
    ```java
    g.V().as("a").out().out().where(P.eq("a")) // is the current entry equal to the entry referred by `a`?
    ```
* startKey - the tag containing the object to filter, </br> predicate - the predicate containing another tag to apply.
    ```java
    g.V().as("a").out().out().as("b").where("b", P.eq("a")) // is the entry referred by `b` equal to the entry referred by `a`?
    ```
The by() can be applied to a number of different steps to alter their behaviors. Here are some usages of the modulated by()-step after a where-step:
* empty - this form is essentially an identity() modulation.
    ```java
    g.V().as("a").out().out().as("b").where("b", P.eq("a")).by() // = g.V().as("a").out().out().as("b").where("b", P.eq("a"))
    ```
* propertyKey - filter by the property value of the specified tag given the property key.
    ```java
    g.V().as("a").out().out().as("b").where("b", P.eq("a")).by("name") // whether entry `b` and entry `a` have the same property value of `name`?
    ```
* traversal - filter by the computed value after applying the specified tag to the nested traversal.
    ```java
    g.V().as("a").out().out().as("b").where("b", P.eq("a")).by(out().count()) // whether entry `b` and entry `a` have the same count of one-hop neighbors?
    ```
#### not(traversal)
The not()-step is opposite to the where()-step and removes objects from the traversal stream when the traversal provided as an argument does not return any objects.

Parameters: </br>
notTraversal - the traversal to filter by.
```java
g.V().not(out().count())
g.V().not(out().count().is(gt(0)))
```
#### dedup()
Remove all duplicates in the traversal stream up to this point.

Parameters:
dedupLabels - composition of the given labels determines de-duplication. No labels implies current object.
```java
g.V().dedup()
g.V().as("a").out().dedup("a") // dedup by entry `a`
g.V().as("a").out().as("b").dedup("a", "b") // dedup by the composition of entry `a` and `b`
```

Usages of the modulated by()-step: </br>
* propertyKey - dedup by the property value of the current object or the specified tag given the property key.
    ```java
    g.V().dedup().by("name") // dedup by the property value of `name` of the current entry
    g.V().as("a").out().dedup("a").by("name") // dedup by the property value of `name` of the entry `a`
    ```
* token - dedup by the token value of the current object or the specified tag.
    ```java
    g.V().dedup().by(T.id)
    g.V().dedup().by(T.label)
    g.V().as("a").out().dedup("a").by(T.id)
    g.V().as("a").out().dedup("a").by(T.label)
    ```
* traversal - dedup by the computed value after applying the current object or the specified tag to the nested traversal.
    ```java
    g.V().dedup().by(out().count())
    g.V().as("a").out().dedup("a").by(out().count())
    ```
### Project
#### id()
The id()-step is meant to map the graph element to its identifier.
```java
g.V().id()
```
#### label()
The label()-step is meant to map the graph element to its label.
```java
g.V().label()
```
#### constant()
The constant()-step is meant to map any object to a fixed object value.

Parameters: </br>
value - a fixed object value.
```java
g.V().constant(1)
g.V().constant("marko")
g.V().constant(1.0)
```
#### valueMap()
The valueMap()-step is meant to map the graph element to a map of the property entries according to their actual properties. If no property keys are provided, then all property values are retrieved.

Parameters: </br>
propertyKeys - the properties to retrieve.
```java
g.V().valueMap()
g.V().valueMap("name")
g.V().valueMap("name", "age")
```
#### values
The values()-step is meant to map the graph element to the values of the associated properties given the provide property keys. Here we just allow only one property key as the argument to the `values()` to implement the step as a map instead of a flat-map, which may be a little different from the standard gremlin.

Parameters: </br>
propertyKey - the property to retrieve its value from.
```java
g.V().values("name")
```
#### select()
The select()-step is meant to map the traverser to the object specified by the selectKey or to a map projection of sideEffect values.

Parameters: </br>
selectKeys - the keys to project.
```java
g.V().as("a").select("a")
g.V().as("a").out().as("b").select("a", "b")
```

Usages of the modulated by()-step: </br>
* empty - an identity() modulation.
    ```java
    g.V().as("a").select("a").by() // = g.V().as("a").select("a")
    g.V().as("a").out().as("b").select("a", "b").by().by() // = g.V().as("a").out().as("b").select("a", "b")
    ```
* token - project the token value of the specified tag.
    ```java
    g.V().as("a").select("a").by(T.id)
    g.V().as("a").select("a").by(T.label)
    ```
* propertyKey - project the property value of the specified tag given the property key.
    ```java
    g.V().as("a").select("a").by("name")
    ```
* traversal - project the computed value after applying the specified tag to the nested traversal.
    ```java
    g.V().as("a").select("a").by(valueMap("name", "id"))
    g.V().as("a").select("a").by(out().count())
    ```
### Aggregate
#### count()
Count the number of traverser(s) up to this point.
```java
g.V().count()
```
#### fold()
Rolls up objects in the stream into an aggregate list.
```java
g.V().limit(10).fold() // select top-10 vertices from the stream and fold them into single list
```
#### sum()
Sum the traverser values up to this point.
```java
g.V().values("age").sum()
```
#### min()
Determines the minimum value in the stream.
```java
g.V().values("age").min()
```
#### max()
Determines the maximum value in the stream.
```java
g.V().values("age").max()
```
#### mean()
Compute the average value in the stream.
```java
g.V().values("age").mean()
```
#### group()
Organize objects in the stream into a Map. Calls to group() are typically accompanied with by() modulators which help specify how the grouping should occur.

Usages of the key by()-step:
* empty - group the elements in the stream by the current value.
    ```java
    g.V().group().by() // = g.V().group()
    ```
* propertyKey - group the elements in the stream by the property value of the current object given the property key.
    ```java
    g.V().group().by("name")
    ```
* traversal - group the elements in the stream by the computed value after applying the current object to the nested traversal.
    ```java
    g.V().group().by(values("name")) // = g.V().group().by("name")
    g.V().group().by(out().count())
    ```

Usages of the value by()-step:
* empty - fold elements in each group into a list, which is a default behavior.
    ```java
    g.V().group().by().by() // = g.V().group()
    ```
* propertyKey - for each element in the group, get their property values according to the given keys.
    ```java
    g.V().group().by().by("name")
    ```
* aggregateFunc - aggregate function to apply in each group.
    ```java
    g.V().group().by().by(count())
    g.V().group().by().by(fold())
    g.V().group().by().by(values("name").fold()) // = g.V().group().by().by("name"), get the property values of `name` of the vertices in each group list
    g.V().group().by().by(values("age").sum()) // sum the property values of `age` in each group
    g.V().group().by().by(values("age").min()) // find the minimum value of `age` in each group
    g.V().group().by().by(values("age").max()) // find the maximum value of `age` in each group
    g.V().group().by().by(values("age").mean()) // calculate the average value of `age` in each group
    g.V().group().by().by(dedup().count()) // count the number of distinct elements in each group
    g.V().group().by().by(dedup().fold()) // de-duplicate in each group list
    ```
#### groupCount()
Counts the number of times a particular objects has been part of a traversal, returning a map where the object is the key and the value is the count.

Usages of the key by()-step:
* empty - group the elements in the stream by the current value.
    ```java
    g.V().groupCount().by() // = g.V().groupCount()
    ```
* propertyKey - group the elements in the stream by the property value of the current object given the property key.
    ```java
    g.V().groupCount().by("name")
    ```
* traversal - group the elements in the stream by the computed value after applying the current object to the nested traversal.
    ```java
    g.V().groupCount().by(values("name")) // = g.V().groupCount().by("name")
    g.V().groupCount().by(out().count())
    ```
### Order
#### order()
Order all the objects in the traversal up to this point and then emit them one-by-one in their ordered sequence.

Usages of the modulated by()-step: </br>
* empty - order by the current object in ascending order, which is a default behavior.
    ```java
    g.V().order().by() // = g.V().order()
    ```
* order - the comparator to apply typically for some order (asc | desc | shuffle).
    ```java
    g.V().order().by(Order.asc) // = g.V().order()
    g.V().order().by(Order.desc)
    ```
* propertyKey - order by the property value of the current object given the property key.
    ```java
    g.V().order().by("name") // default order is asc
    g.V().order().by("age")
    ```
* traversal - order by the computed value after applying the current object to the nested traversal.
    ```java
    g.V().order().by(out().count()) // default order is asc
    ```
* propertyKey - order by the property value of the current object given the property key, </br> order - the comparator to apply typically for some order.
    ```java
    g.V().order().by("name", Order.desc)
    ```
* traversal - order by the computed value after applying the current object to the nested traversal, </br> order - the comparator to apply typically for some order.
    ```java
    g.V().order().by(out().count(), Order.desc)
    ```
### Statistics
#### limit()
Filter the objects in the traversal by the number of them to pass through the stream, where only the first n objects are allowed as defined by the limit argument.

Parameters: </br>
limit - the number at which to end the stream.
```java
g.V().limit(10)
```
#### coin()
Filter the object in the stream given a biased coin toss.

Parameters: </br>
probability - the probability that the object will pass through.
```java
g.V().coin(0.2) // range is [0, 1]
```
### Union
#### union()
Merges the results of an arbitrary number of traversals.

Parameters: </br>
unionTraversals - the traversals to merge.
```java
g.V().union(out(), out().out())
```
### Match
#### match()
The match()-step provides a declarative form of graph patterns to match with. With match(), the user provides a collection of "sentences," called patterns, that have variables defined that must hold true throughout the duration of the match(). For most of the complex graph patterns, it is usually much easier to express via match() than with single-path traversals.

Parameters: </br>
matchSentences - define a collection of patterns. Each pattern consists of a start tag, a serials of gremlin steps (binders) and an end tag. 

Supported binders within a pattern: </br>
* Expand: in()/out()/both(), inE()/outE()/bothE(), inV()/outV()/otherV/bothV
* PathExpand
* Filter: has()/not()/where
```java
g.V().match(__.as("a").out().as("b"), __.as("b").out().as("c"))
g.V().match(__.as("a").out().out().as("b"), where(__.as("a").out().as("b")))
g.V().match(__.as("a").out().out().as("b"), not(__.as("a").out().as("b")))
g.V().match(__.as("a").out().has("name", "marko").as("b"), __.as("b").out().as("c"))
```
### Subgraph
#### subgraph()
an edge-induced subgraph extracted from the original graph.

Parameters: </br>
graphName - the name of the side-effect key that will hold the subgraph.
```java
g.E().subgraph("all") 
g.V().has('name', "marko").outE("knows").subgraph("partial")
```
## Ext-Steps
The following steps are extended to denote more complex situations.
### PathExpand
In Graph querying, expanding a multiple-hops path from a starting point is called `PathExpand`, which is commonly used in graph scenarios. In addition, there are different requirements for expanding strategies in different scenarios, i.e. it is required to output a simple path or all vertices explored along the expanding path. We introduce the with()-step to configure the corresponding behaviors of the `PathExpand`-step.

#### out()
Expand a multiple-hops path along the outgoing edges, which length is within the given range.

Parameters: </br>
lengthRange - the lower and the upper bounds of the path length, </br> edgeLabels - the edge labels to traverse.

Usages of the with()-step: </br> 
keyValuePair - the options to configure the corresponding behaviors of the `PathExpand`-step.
```java
g.V().out("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'END_V') // expand hops within the range of [1, 10) along the outgoing edges, vertices can be duplicated and only the end vertex should be kept

g.V().out("1..10").with('PATH_OPT', 'SIMPLE').with('RESULT_OPT', 'ALL_V') // expand hops within the range of [1, 10) along the outgoing edges, vertices can not be duplicated and all vertices should be kept

g.V().out("1..10") // = g.V().out("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'END_V')

g.V().out("1..10", "knows") // expand hops within the range of [1, 10) along the outgoing edges which label is `knows`, vertices can be duplicated and only the end vertex should be kept

g.V().out("1..10", "knows", "created") // expand hops within the range of [1, 10) along the outgoing edges which label is `knows` or `created`, vertices can be duplicated and only the end vertex should be kept
``` 
#### in()
Expand a multiple-hops path along the incoming edges, which length is within the given range.
```java
g.V().in("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'END_V')
```
#### both()
Expand a multiple-hops path along the incident edges, which length is within the given range.
```java
g.V().both("1..10").with('PATH_OPT', 'ARBITRARY').with('RESULT_OPT', 'END_V')
```
#### endV()
By default, all kept vertices are stored in a path collection which can be unfolded by a `endV()`-step.
```java
g.V().out("1..10").with('RESULT_OPT', 'ALL_V') // a path collection containing the vertices within [1, 10) hops
g.V().out("1..10").with('RESULT_OPT', 'ALL_V').endV() // unfold vertices in the path collection
```
### Expression
Expression is introduced to denote property-based calculations or filters, which consists of the following basic entries:
```java
@ // the value of the current entry
@.name // the property value of `name` of the current entry
@a // the value of the entry `a`
@a.name // the property value of `name` of the entry `a`
```

And related operations can be performed based on these entries, including:
* arithmetic
    ```java
    @.age + 10
    @.age * 10
    (@.age + 4) / 10 + (@.age - 5)
    ```
* logic comparison
    ```java
    @.name == "marko"
    @.age != 10
    @.age > 10
    @.age < 10
    @.age >= 10
    @.weight <= 10.0
    ```
* logic connector
    ```java
    @.age > 10 && @.age < 20
    @.age < 10 || @.age > 20
    ```
* bit manipulation
    ```java
    @.num | 2
    @.num & 2
    @.num ^ 2
    @.num >> 2
    @.num << 2
    ```
* exponentiation
    ```java
    @.num ^^ 3 
    @.num ^^ -3
    ```

Expression(s) in project or filter:
* filter: where(expr("..."))
    ```java
    g.V().where(expr("@.name == \"marko\"")) // = g.V().has("name", "marko")
    g.V().where(expr("@.age > 10")) // = g.V().has("age", P.gt(10))
    g.V().as("a").out().where(expr("@.name == \"marko\" || (@a.age > 10)"))
    ```
* project: select(expr("..."))
    ```java
    g.V().select(expr("@.name")) // = g.V().values("name")
    ```
### Aggregate (Group)
The group()-step in standard gremlin has limited capabilities (i.e. grouping can only be performed based on a single key, and only one aggregate calculation can be applied in each group), which cannot be applied to the requirements of performing group calculations on multiple keys or values; Therefore, we further extend the capabilities of the group()-step, allowing multiple variables to be set and different aliases to be configured in key by()-step and value by()-step respectively.

Usages of the key by()-step:
```java
group().by(values("name").as("k1"), values("age").as("k2")) // group by the property values of `name` and `age` of the current entry
group().by(out().count().as("k1"), values("name").as("k2")) // group by the count of one-hop neighbors and the property value of `age` of the current entry
```

Usages of the value by()-step:
```java
group().by("name").by(count().as("v1"), values("age").sum().as("k2")) // calculate the count of vertices and the sum of `age` respectively in each group
```
## Limitations
Here we list steps which are unsupported yet. Some will be supported in the near future while others will remain unsupported for some reasons.

### Todo
The following steps will be supported in the near future.
#### identity()
Map the current object to itself.
```java
g.V().identity()
g.V().union(identity(), out().out())
```
#### elementMap()
Map the graph element to a map of T.id, T.label and the property values according to the given keys. If no property keys are provided, then all property values are retrieved.

Parameters: </br>
propertyKeys - the properties to retrieve.
```java
g.V().elementMap()
```
#### path()
Map the traverser to its path history.
```java
g.V().out().out().path()
g.V().as("a").out().out().select("a").by("name").path()
```
#### unfold()
Unrolls a iterator, iterable or map into a linear form.
```java
g.V().fold().unfold()
```
#### local()
```java
g.V().fold().count(local)
g.V().values('age').fold().sum(local)
```
### Others
The following steps will remain unsupported.
#### repeat()
* repeat().times() </br>
In graph pattern scenarios, `repeat().times()` can be replaced equivalently by the `PathExpand`-step.
    ```java
    g.V().repeat(out("knows")).times(2) // = g.V().out("1..3", "knows").endV()
    g.V().repeat(out("knows").simplePath()).times(2) // = g.V().out("1..3", "knows").with('PATH_OPT', 'SIMPLE').endV()
    ```
* repeat().until() </br>
It is a imperative syntax, not declarative.
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