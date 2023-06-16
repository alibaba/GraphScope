# Cypher Support
We report the current states of GIE supporting the Cypher queries, mainly in terms of
data types and operators.
## Data Types
As [Neo4j](https://neo4j.com/docs/cypher-manual/current/values-and-types), we have provided support for
data value of types in the categories of **property**, **structural** and **constructed**.
However, the specific data types that we support are slightly modified from those in Cypher to ensure compatibility with our storage system. Further details will be elaborated upon.

### Property Types
The available data types stored in the vertices (equivalent of nodes in Cypher) and edges (equivalent of relationships in Cypher), known as property types, are divided into several categories including Boolean, Integer, Float, Numeric, String, and Temporal. These property types are extensively utilized and can be commonly utilized in queries and as parameters -- making them the most commonly used data types.

| Category  | Cypher Type | GIE Type  | Supported  |  Todo  |
|:---|:---|:---|:---:|:---|
| Boolean  | BOOLEAN  | bool |  <input type="checkbox" disabled checked /> |   |
| Integer  | INTEGER | int32/uint32/int64/uint64 | <input type="checkbox" disabled checked /> |   |
| Float  | FLOAT | float/double |  <input type="checkbox" disabled checked />  |   |
| String | STRING | String | <input type="checkbox" disabled checked />  |    |
| Bytes| BYTES | Bytes | <input type="checkbox" disabled checked />  |   |
| Placeholder | NULL | None |  <input type="checkbox" disabled  />  |   Planned  |
| Temporal | DATE | Date | <input type="checkbox" disabled  />  |  Planned  |
| Temporal | DATETIME (ZONED) | DateTime (Zoned) | <input type="checkbox" disabled  />  | Planned    |
| Temporal | TIME (ZONED) | DateTime (Zoned) | <input type="checkbox" disabled  />  | Planned  |

### Structural types
In a graph, Structural Types are the first-class citizens and are comprised of the following:
- Vertex: It encodes the information of a particular vertex in the graph. The information includes the id, label, and a map of properties. However, it is essential to note that multiple labels in a vertex are currently unsupported in GIE.
- Edge: It encodes the information of a particular edge in the graph. The information comprises the id, edge label, a map of properties, and a pair of vertex ids that refer to source/destination vertices.
- Path: It encodes the alternating sequence of vertices and conceivably edges while traversing the graph.

| Cypher Type | GIE Type | Supported  |  Todo  |
|:---|:---|:---:|:---|
| NODE   | Vertex  |  <input type="checkbox" disabled checked /> |   |
| RELATIONSHIP  | Edge  |  <input type="checkbox" disabled checked /> |   |
| PATH  | Path  |  <input type="checkbox" disabled checked /> |   |

### Constructed Types
Constructed types mainly include PAIR, LIST and MAP.

| Category  | Cypher Type | GIE Type  | Supported  |  Todo  |
|:---|:---|:---|:---:|:---|
| PAIR  | POINT | Pair |  <input type="checkbox" disabled checked /> |   |
| LIST | LIST<INNER_TYPE> | int32/int64/double/string/pair Array |  <input type="checkbox" disabled checked /> |   |
| Map  | MAP  | N/A |  <input type="checkbox" disabled  />| only used in Vertex/Edge  |

## Operators
A notable limitation for now is that we do not
allow specifying multiple `MATCH` clauses in **one** query. For example,
the following code will not compile:
```Cypher
MATCH (a) -[]-> (b)
WITH a, b
MATCH (a) -[]-> () -[]-> (b)  # second MATCH clause
RETURN a, b;
```

### Clause
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

### Builtin Functions, Expression and Logical Operations
| Category  |  Operation | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Aggregate | avg()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | min()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | max()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | count()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | count(distinct)  |  <input type="checkbox" disabled checked /> |   |
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
