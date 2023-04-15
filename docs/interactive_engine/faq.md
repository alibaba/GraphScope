# Frequently Asked Questions (FAQs) for GIE Gremlin Usage

## What's the difference between Inner ID and Property ID ?

The main difference between Inner ID and Property ID is that Inner ID is a system-assigned identifier used internally by the graph engine for efficient data storage and retrieval, while Property ID is a user-defined property within a specific entity type. 

For example, in the LDBC (Linked Data Benchmark Council) schema, we have an entity type called 'PERSON', which has its own list of properties, consisting of 'id', 'name' and 'birthday'. In the actual storage, we maintain key-value pairs for each instance of entity type 'PERSON', and internally maintain a unique ID to differentiate each such instance. The unique ID in this context is referred to as the Inner ID, and the 'id' in the attribute list is the Property ID.

GIE Gremlin provides different approaches to query a vertex instance by its Inner ID or Property ID, similar to:
```scss
// by its inner id
g.V(1)
g.V().hasId(1)

// by its property id 
g.V().has('id', 1)
```

For edges, we do not currently provide any approaches to query based on Inner ID, for two reasons: 
- Firstly, Inner ID is internally maintained by the system and should not be exposed to users by default. 
- Secondly, a single edge instance may not be uniquely identified by Inner ID alone, as it typically requires a triplet such as \<src, dst, edge\>.

## How to use path expand in GIE Gremlin ?

With path_expand, users can define their desired path pattern concretely and further define corresponding characteristics based on that path. For example, if an entity of type 'PERSON' wants to find instances that can be reached by 3-hops, in traditional Gremlin, it can only be represented as ```g.V().hasLabel('PERSON').both().both().both()```. With path_expand, it can be represented more concisely as ```g.V().hasLabel('PERSON').both('3..4').endV()```, where ```both('3..4')``` represents the path pattern, and ```'3..4'``` specifies the range of hops as [3, 4).

We can further define characteristics of the path pattern using path_expand. For example, if an entity of type 'PERSON' wants to find instances that can be reached by 3-hops, while ensuring that the path is a simple path (no repeated vertices or edges), it can be represented as:
```scss
g.V().hasLabel('PERSON').both('3..4').with('PATH_OPT', 'SIMPLE').endV()
```

You can refer to [PathExpand](https://github.com/alibaba/GraphScope/blob/main/docs/interactive_engine/supported_gremlin_steps.md#pathexpand) for more examples and usage of path_expand.

## How to filter data in GIE Gremlin like SQL ?

With ```expr```, We can support SQL-like expressions in GIE Gremlin. For example, if we want to find all 'PERSON' instances with either the name 'marko' or the age '27', we can represent it as follows:
```scss
g.V().hasLabel('PERSON').where(expr('@.name=\"marko\" || @.age = 27'))
```
In traditional Gremlin, it can only be represented as follows:
```scss
g.V().hasLabel('PERSON').has('name', 'marko').or().has('age', 27)
```
It is equivalent to the following SQL-like expression:
```scss
SELECT *
FROM PERSON
WHERE name = 'marko' OR age = 27;
```

Traditional Gremlin uses the ```HasStep``` operator to support filter queries, which has some limitations compared to the ```Where``` operator in SQL:
- ```HasStep``` can only express query filters based on the current vertex or edge and their properties, without the ability to cross multiple vertices or edges.
- On the other hand, ```HasStep``` in Gremlin for complex expressions may not be as intuitive as in SQL.

We have addressed the limitations and shortcomings of Gremlin in filter expression by using ```expr```, for more usage, please refer to [Expression](https://github.com/alibaba/GraphScope/blob/main/docs/interactive_engine/supported_gremlin_steps.md#expression).

## How to aggregate data in GIE Gremlin like SQL?

We further extend the ```group``` operator in Gremlin to support multi-column grouping operations, similar to those in SQL.
### group by multiple keys
```scss
g.V().hasLabel('PERSON').groupCount().by('name', 'age')
```
which is equivalent to:
```scss
SELECT 
  PERSON.name, 
  PERSON.age, 
  COUNT(*) 
FROM 
  PERSON 
GROUP BY 
  PERSON.name, 
  PERSON.age
```
### group by multiple values:
```scss
g.V()
  .hasLabel('PERSON')
  .group()
    .by('name')
    .by(count('age').as('age_cnt'), sum('age').as('age_sum'))
```
which is equivalent to :
```scss
SELECT 
  PERSON.name, 
  COUNT(age) AS age_cnt, 
  SUM(age) AS age_sum 
FROM 
  PERSON 
GROUP BY 
  name
```
Please refer to [Aggregate](https://github.com/alibaba/GraphScope/blob/main/docs/interactive_engine/supported_gremlin_steps.md#aggregate-group) for more usage.

## How to optimize Gremlin queries for performance in GIE?

### Use appropriate indexing
GIE supports various indexing options such as vertex label index, primary key index, and edge label index. Properly defining and using indexes can significantly improve query performance.

For example, in the LDBC schema, we define the property ID as the primary key for the entity type 'PERSON' and maintain the corresponding primary key index in the storage. This allows us to directly index specific 'PERSON' instances using \<Label, Property ID\>, without scanning all vertices and filtering based on property key-value. This can be expressed in a Gremlin query as follows:
```scss
g.V().hasLabel('PERSON').has('id', propertyIdValue)
```
Where 'id' is the property ID, and 'propertyIdValue' is the value of the property key. By directly using the primary key index, query performance can be significantly improved, avoiding full table scans and property value filtering, thereby optimizing query performance.

## How to use subgraph in GIE Gremlin ?

Subgraph in GIE is edge-induced, which means it is formed by selecting a subset of edges from a larger graph, along with their incident vertices.

Therefore, You can only perform subgraph operations after edge-output operators like E(), outE(), inE() or bothE(). Here's an example:

```scss
g.V().outE().limit(10).subgraph('sub_graph').count()
```
Please refer to [Subgraph](https://github.com/alibaba/GraphScope/blob/main/docs/interactive_engine/supported_gremlin_steps.md#subgraph) for more usage.