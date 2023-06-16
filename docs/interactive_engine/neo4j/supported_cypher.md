# Cypher Support
We report the current capabilities of GIE, concerning its support for Neo4j's Cypher queries. This includes the data types, operators and clauses that it supports. Note that we have also included the functionalities that we plan to support in the near future as "Planned" features.
In this document, all terminologies including data types, operators, and keywords in clauses are case-insensitive. However, for clarity, we have chosen to use capital and lowercase letters for the terminologies of Neo4j and GIE, respectively.

## Data Types
As [Neo4j](https://neo4j.com/docs/cypher-manual/current/values-and-types), we have provided support for
data value of types in the categories of **property**, **structural** and **constructed**.
However, the specific data types that we support are slightly modified from those in Cypher to ensure compatibility with our storage system. Further details will be elaborated upon.

### Property Types
The available data types stored in the vertices (equivalent of nodes in Cypher) and edges (equivalent of relationships in Cypher), known as property types, are divided into several categories including Boolean, Integer, Float, String, Bytes, Placeholder and Temporal. These property types are extensively utilized and can be commonly utilized in queries and as parameters -- making them the most commonly used data types.

| Category  | Cypher Type | GIE Type  | Supported  |  Todo  |
|:---|:---|:---|:---:|:---|
| Boolean  | BOOLEAN  | bool |  <input type="checkbox" disabled checked /> |   |
| Integer  | INTEGER | int32/uint32/int64/uint64 | <input type="checkbox" disabled checked /> |   |
| Float  | FLOAT | float/double |  <input type="checkbox" disabled checked />  |   |
| String | STRING | string | <input type="checkbox" disabled checked />  |    |
| Bytes| BYTES | bytes | <input type="checkbox" disabled checked />  |   |
| Placeholder | NULL | none |  <input type="checkbox" disabled  />  |   Planned  |
| Temporal | DATE | date | <input type="checkbox" disabled  />  |  Planned  |
| Temporal | DATETIME (ZONED) | datetime (Zoned) | <input type="checkbox" disabled  />  | Planned    |
| Temporal | TIME (ZONED) | time (Zoned) | <input type="checkbox" disabled  />  | Planned  |

### Structural types
In a graph, Structural Types are the first-class citizens and are comprised of the following:
- Vertex: It encodes the information of a particular vertex in the graph. The information includes the id, label, and a map of properties. However, it is essential to note that multiple labels in a vertex are currently unsupported in GIE.
- Edge: It encodes the information of a particular edge in the graph. The information comprises the id, edge label, a map of properties, and a pair of vertex ids that refer to source/destination vertices.
- Path: It encodes the alternating sequence of vertices and conceivably edges while traversing the graph.

|Category | Cypher Type | GIE Type | Supported  |  Todo  |
|:---|:---|:---|:---:|:---|
|Graph | NODE   | vertex  |  <input type="checkbox" disabled checked /> |   |
|Graph | RELATIONSHIP  | edge  |  <input type="checkbox" disabled checked /> |   |
|Graph | PATH  | path  |  <input type="checkbox" disabled checked /> |   |

### Constructed Types
Constructed types mainly include the categories of Array and Map.

| Category  | Cypher Type | GIE Type  | Supported  |  Todo  |
|:---|:---|:---|:---:|:---|
| Array | LIST<INNER_TYPE> | int32/int64/double/string/pair Array |  <input type="checkbox" disabled checked /> |   |
| Map  | MAP  | N/A |  <input type="checkbox" disabled  />| only used in Vertex/Edge  |

## Operators
We list GIE's support of the operators in the categories of Aggregation, Property, Mathematical,
Comparison, String and Boolean. Examples and functionalities of these operators are the same
as in [Neo4j](https://neo4j.com/docs/cypher-manual/current/syntax/operators/).
Note that some Aggregator operators, such as `max()`, we listed here are implemented in Neo4j as
[functions](https://neo4j.com/docs/cypher-manual/current/functions/). We have not introduced functions at this moment.


| Category  |  Cypher Operation | GIE Operation | Supported  |  Todo  |
|:---|:---|:----|:---:|:---|
| Aggregate | AVG() | avg()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | MIN() | min()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | MAX() | max()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | COUNT() | count()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | COUNT(DISTINCT) | count(distinct)  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | sum()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | collect()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | collect(distinct)  |  <input type="checkbox" disabled checked /> |   |
| Logical | =, <>, >, <, >=, <= |  <input type="checkbox" disabled checked /> |   |
| Logical | AND, OR |  <input type="checkbox" disabled checked /> |   |
| Logical | NOT, IN |  <input type="checkbox" disabled  />|  planned |
| String Match | START WITH |  <input type="checkbox" disabled  />|  planned |
| String Match | END WITH |  <input type="checkbox" disabled  />|  planned |
| String Match | CONTAINS |  <input type="checkbox" disabled  />|  planned |
| String Match (Reg) | =~ |  not planned |
| Arithmetic  | Add (+) |  <input type="checkbox" disabled checked /> |  |
| Arithmetic  | Subtract (-) |  <input type="checkbox" disabled checked /> |  |
| Arithmetic  | Multiply (*) |  <input type="checkbox" disabled checked /> |  |
| Arithmetic  | Divide (/) |  <input type="checkbox" disabled checked /> |  |
| Arithmetic  | Mod (%) |  <input type="checkbox" disabled checked /> |  |
| Arithmetic  | Exponential (^) |  <input type="checkbox" disabled checked /> |  |
| BitOpr  | AND (&), OR (\|), NOT(~) |  <input type="checkbox" disabled checked /> |  |
| BitOpr  | LEFT SHIFT (<<) |  <input type="checkbox" disabled  />| planned |
| BitOpr  | RIGHT SHIFT (>>) | <input type="checkbox" disabled  />| planned |
| Branch | CASE WHEN  |  <input type="checkbox" disabled  />| planned |



## Clause
A notable limitation for now is that we do not
allow specifying multiple `MATCH` clauses in **one** query. For example,
the following code will not compile:
```Cypher
MATCH (a) -[]-> (b)
WITH a, b
MATCH (a) -[]-> () -[]-> (b)  # second MATCH clause
RETURN a, b;
```

| Keyword |  Supported  |  Todo  | Desc.|
|:---|:---:|:---|:---|
| MATCH | <input type="checkbox" disabled checked />  | |  only one Match clause is allowed |
| OPTIONAL MATCH | <input type="checkbox" disabled  /> |  planned | implements as left outer join  |
| RETURN | <input type="checkbox" disabled checked />  |   |   |
| WITH | <input type="checkbox" disabled checked />  |   | project, aggregate, distinct |
| WHERE | <input type="checkbox" disabled checked />  |    |   |
| NOT EXIST (an edge/path) | <input type="checkbox" disabled  />| implements as anti join | planned
| ORDER BY | <input type="checkbox" disabled checked />  |  |   |
| LIMIT | <input type="checkbox" disabled checked />  |    |   |

