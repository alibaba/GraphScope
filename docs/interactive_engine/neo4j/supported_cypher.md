# Cypher Support
We report the current states of GIE supporting the Cypher queries, regarding
data types and operators. A notable limitation for now is that we do not
allow specifying multiple `MATCH` clauses in **one** query. For example,
the following code will not compile:
```cypher
MATCH (a) -[]-> (b)
WITH a, b
MATCH (a) -[]-> () -[]-> (b)  # second MATCH clause
RETURN a, b;
```
## Data Types
As [Cypher](https://neo4j.com/docs/cypher-manual/current/values-and-types), we have provided support for
data value of types in the categories of **property**, **structural** and **constructed**.
However, the specific data types that we support are slightly modified from those in Cypher to ensure compatibility with our storage system. Further details will be elaborated upon.

### Property Types
The available data types stored in the vertices (equivalent of nodes in Cypher) and edges (equivalent of relationships in Cypher), known as property types, are divided into several categories including Boolean, Integer, Float, Numeric, String, and Temporal. These property types are extensively utilized and can be commonly utilized in queries and as parameters -- making them the most commonly used data types.

| Category  | Cypher Type | GIE Type  | Supported  |  Todo  |
|:---|:---|:---|:---|:---|
| Boolean  | BOOL  | bool |  <input type="checkbox" disabled checked /> |   |
| Integer  | INTEGER | int32/uint32/int64/uint64 | <input type="checkbox" disabled checked /> |   |
| Float  | FLOAT | float/double |  | <input type="checkbox" disabled checked />  |   |
| String | String | String | <input type="checkbox" disabled checked />  |    |
| Temporal | Date | Date | <input type="checkbox" disabled  />  |  Planned  |
| Temporal | DateTime (Zoned) | DateTime (Zoned) | <input type="checkbox" disabled  />  | Planned    |
| Temporal | Time (Zoned) | DateTime (Zoned) | <input type="checkbox" disabled  />  | Planned  |

### Structural types

| Category  |  Type  | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Graph  | Vertex  |  <input type="checkbox" disabled checked /> |   |
| Graph  | Edge  |  <input type="checkbox" disabled checked /> |   |
| Graph  | Path  |  <input type="checkbox" disabled checked /> |   |

### Constructed Types
| Category  |  Type  | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Pair  | Pair  |  <input type="checkbox" disabled checked /> |   |
| Array  | int32 Array  |  <input type="checkbox" disabled checked /> |   |
| Array  | int64 Array  |  <input type="checkbox" disabled checked /> |   |
| Array  | double Array  |  <input type="checkbox" disabled checked /> |   |
| Array  | string Array  |  <input type="checkbox" disabled checked /> |   |
| Array  | pair Array  |  <input type="checkbox" disabled checked /> |   |
| Map  | Map  |  <input type="checkbox" disabled  />| not planned  |

## Operators

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
