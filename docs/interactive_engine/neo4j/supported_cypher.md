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


### Primitive Types

| Category  |  Type  | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Bool  | bool  |  &#9745 |   |
| Integer  |  int32/uint32/int64/uint64 | &#9745 |   |
| Float  | float/double  | &#9745  |   |
| Numeric  | numeric(prec, scale)   |  &#9744  |  Planned   |
| String | String  | &#9745  |    |
| String | Char(n) | &#9744   | Planned  |
| String | VarChar(n) | &#9744   | Planned |
| Temporal | Date | &#9744   |  Planned  |
| Temporal | DateTime (Zone) | &#9744   | Planned    |
| Temporal | Time (Zone) | &#9744   | Planned  |

### Graph Elements

| Category  |  Type  | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Graph  | Vertex  |  &#9745 |   |
| Graph  | Edge  |  &#9745 |   |
| Graph  | Path  |  &#9745 |   |

### Composite Types
| Category  |  Type  | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Pair  | Pair  |  &#9745 |   |
| Array  | int32 Array  |  &#9745 |   |
| Array  | int64 Array  |  &#9745 |   |
| Array  | double Array  |  &#9745 |   |
| Array  | string Array  |  &#9745 |   |
| Array  | pair Array  |  &#9745 |   |
| Map  | Map  |  &#9744 | not planned  |

## Operators

### Clause
| Keyword |  Supported  |  Todo  | Desc.|
|:---|:---:|:---|:---|
| MATCH | &#9745  | |  only one Match clause is allowed |
| OPTIONAL MATCH | &#9744  |  planned | implements as left outer join  |
| RETURN | &#9745  |   |   |
| WITH | &#9745  |   | project, aggregate, distinct |
| WHERE | &#9745  |    |   |
| NOT EXIST (an edge/path) | &#9744 | implements as anti join | planned
| ORDER BY | &#9745  |  |   |
| LIMIT | &#9745  |    |   |

### Builtin Functions, Expression and Logical Operations
| Category  |  Operation | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Aggregate | avg()  |  &#9745 |   |
| Aggregate | min()  |  &#9745 |   |
| Aggregate | max()  |  &#9745 |   |
| Aggregate | count()  |  &#9745 |   |
| Aggregate | count(distinct)  |  &#9745 |   |
| Aggregate | sum()  |  &#9745 |   |
| Aggregate | collect()  |  &#9745 |   |
| Aggregate | collect(distinct)  |  &#9745 |   |
| Logical | =, <>, >, <, >=, <= |  &#9745 |   |
| Logical | AND, OR |  &#9745 |   |
| Logical | NOT, IN |  &#9744 |  planned |
| String Match | START WITH |  &#9744 |  planned |
| String Match | END WITH |  &#9744 |  planned |
| String Match | CONTAINS |  &#9744 |  planned |
| String Match (Reg) | =~ |  not planned |
| Arithmetic  | Add (+) |  &#9745 |  |
| Arithmetic  | Subtract (-) |  &#9745 |  |
| Arithmetic  | Multiply (*) |  &#9745 |  |
| Arithmetic  | Divide (/) |  &#9745 |  |
| Arithmetic  | Mod (%) |  &#9745 |  |
| Arithmetic  | Exponential (^) |  &#9745 |  |
| BitOpr  | AND (&), OR (\|), NOT(~) |  &#9745 |  |
| BitOpr  | LEFT SHIFT (<<) |  &#9744 | planned |
| BitOpr  | RIGHT SHIFT (>>) | &#9744 | planned |
| Branch | CASE WHEN  |  &#9744 | planned |
