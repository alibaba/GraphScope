# Cypher Support
This document outlines the current capabilities of GIE in supporting Neo4j's Cypher queries and
compares them to the [syntax](https://neo4j.com/docs/cypher-manual/current/syntax/) specified in Neo4j.
While our goal is to comply with Neo4j's syntax, GIE currently has some limitations.
One major constraint is that we solely support the **read** path in Cypher.
Therefore, functionalities associated with writing, such as adding vertices/edges or modifying their properties, remain **unaddressed**.

We provide in-depth details regarding Cypher's support in GIE, mainly including data types, operators and clauses.
We further highlight planned features that we intend to offer in the near future.
While all terminologies, including data types, operators, and keywords in clauses, are case-insensitive in this document, we use capital and lowercase letters for the terminologies of Neo4j and GIE, respectively, to ensure clarity.

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
| Bytes| BYTE_ARRAY | bytes | <input type="checkbox" disabled checked />  |   |
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


| Category  | Description | Cypher Operation | GIE Operation | Supported  |  Todo  |
|:---|:----|:---|:----|:---:|:---|
| Aggregate | Average value |  AVG() | avg()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | Minimum value | MIN() | min()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | Maximum value |MAX() | max()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | Count the elements |COUNT() | count()  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | Count the distinct elements | COUNT(DISTINCT) | count(distinct)  |  <input type="checkbox" disabled checked /> |   |
| Aggregate | Summarize the value | SUM()  | sum() |  <input type="checkbox" disabled checked /> |   |
| Aggregate | Collect into a list | COLLECT()  | collect() |  <input type="checkbox" disabled checked /> |   |
| Aggregate | Collect into a set | COLLECT(DISTINCT)  | collect(distinct) | <input type="checkbox" disabled checked /> |   |
| Property | Get property of a vertex/edge | [N\|R]."KEY"  | [v\|e]."key" | <input type="checkbox" disabled checked /> |   |
| Mathematical | Addition |  + |  + | <input type="checkbox" disabled checked /> |  |
| Mathematical | Subtraction |  - |  - | <input type="checkbox" disabled checked /> |  |
| Mathematical  | Multiplication | * | * | <input type="checkbox" disabled checked /> |  |
| Mathematical  | Division  | /  | / | <input type="checkbox" disabled checked /> |  |
| Mathematical  | Modulo division | %  | % |  <input type="checkbox" disabled checked /> |  |
| Mathematical  | Exponentiation | ^ | ^^ |  <input type="checkbox" disabled checked /> |  |
| Comparison | Equality | = | = |  <input type="checkbox" disabled checked /> |  |
| Comparison | Inequality| <> | <> |  <input type="checkbox" disabled checked /> |  |
| Comparison | Less than | < | < |  <input type="checkbox" disabled checked /> |  |
| Comparison | Less than or equal | <= | <= |  <input type="checkbox" disabled checked /> |  |
| Comparison | Greater than | > | > |  <input type="checkbox" disabled checked /> |  |
| Comparison | Greater than or equal | >= | >= |  <input type="checkbox" disabled checked /> |  |
| Comparison | Verify as `NULL`| IS NULL | is null |  <input type="checkbox" disabled checked /> |  |
| Comparison | Verify as `NOT NULL`| IS NOT NULL | is not null |  <input type="checkbox" disabled checked /> |  |
| Comparison | String starts with | STARTS WITH | starts with  | <input type="checkbox" disabled  />|  planned |
| Comparison | String ends with | ENDS WITH | ends with | <input type="checkbox" disabled  />|  planned |
| Comparison | String contains | CONTAINS | contains | <input type="checkbox" disabled  />|  planned |
| Boolean | Conjunction | AND | and |  <input type="checkbox" disabled checked /> |   |
| Boolean | Disjunction | OR | or |  <input type="checkbox" disabled checked /> |   |
| Boolean | Exclusive Disjunction | XOR | xor |  <input type="checkbox" disabled /> | planned |
| Boolean | Negation | NOT | not |  <input type="checkbox" disabled /> | planned  |
| BitOpr  | Bit and | via function | & |  <input type="checkbox" disabled checked /> |  |
| BitOpr  | Bit or | via function | \| |  <input type="checkbox" disabled checked /> |  |
| Boolean | Bit xor | via function | ^ |  <input type="checkbox" disabled checked /> |   |
| BitOpr  | Bit reverse | via function | ~ |  <input type="checkbox" disabled checked /> |  |
| BitOpr  | Bit left shift | via function | << |  <input type="checkbox" disabled  />| planned |
| BitOpr  | Bit right shift | via function | >> |  <input type="checkbox" disabled  />| planned |
| Branch | Use with `Project` and `Return` | CASE WHEN  | CASE WHEN |  <input type="checkbox" disabled  />| planned |
| Scalar | Returns the length of a path | length() | length() | <input type="checkbox" disabled checked /> | |
| ListLiteral | Fold expressions into a single list | [] | [] | <input type="checkbox" disabled checked /> |   |
| MapLiteral | Fold expressions with keys into a single map | {} | {} | <input type="checkbox" disabled checked /> |   |
| Labels | Get label name of a vertex type | labels() | labels() | <input type="checkbox" disabled checked /> |   |
| Type | Get label name of an edge type | type() | type() | <input type="checkbox" disabled checked /> |   |
| Extract | Get interval value from a temporal type | \<temporal\>.\<interval\> | \<temporal\>.\<interval\> | <input type="checkbox" disabled checked /> |   |
| Starts With | Perform case-sensitive matching on the beginning of a string | STARTS WITH | STARTS WITH | <input type="checkbox" disabled checked /> | |
| Ends With | Perform case-sensitive matching on the ending of a string | ENDS WITH | ENDS WITH | <input type="checkbox" disabled checked /> | |
| Contains | Perform case-sensitive matching regardless of location within a string | CONTAINS | CONTAINS | <input type="checkbox" disabled checked /> | |


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

| Keyword | Comments |  Supported  |  Todo
|:---|---|:---:|:---|
| MATCH | only one Match clause is allowed  | <input type="checkbox" disabled checked />  |
| OPTIONAL MATCH | implements as left outer join | <input type="checkbox" checked /> |  |
| RETURN .. [AS] |  | <input type="checkbox" disabled checked />  |   |
| WITH .. [AS] | project, aggregate, distinct | <input type="checkbox" disabled checked />  | |
| WHERE |  | <input type="checkbox" disabled checked />  |    |
| WHERE NOT EXIST (an edge/path) | implements as anti join  |  <input type="checkbox" checked  />|  |
| ORDER BY |  | <input type="checkbox" disabled checked />  |  |
| LIMIT |  | <input type="checkbox" disabled checked />  |    |

