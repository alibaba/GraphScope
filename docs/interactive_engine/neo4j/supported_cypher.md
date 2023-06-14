# Cypher Support
We report the current states of GIE supporting the Cypher queries, regarding
data types and operators. A notable limitation for now is that we do not
allow specifying multiple `MATCH` clauses in **one** query. For example,
the following code will not compile:
```cypher
MATCH (a) -[]-> (b)
with a, b
MATCH (a) -[]-> () -[]-> (b)  # second MATCH clause
RETURN a, b;
```

## Data Types


### Primitive Types

| Category  |  Type  | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Bool  | bool  |  :heavy_check_mark: |   |
| Integer  |  int32/uint32/int64/uint64 | :heavy_check_mark: |   |
| Float  | float/double  | :heavy_check_mark:  |   |
| Numeric  | numeric(prec, scale)   |  :x:  |  Planned   |
| String | String  | :heavy_check_mark:  |    |
| String | Char(n) | :x:   | Planned  |
| String | VarChar(n) | :x:   | Planned |
| Temporal | Date | :x:   |  Planned  |
| Temporal | DateTime (Zone) | :x:   | Planned    |
| Temporal | Time (Zone) | :x:   | Planned  |

### Graph Elements

| Category  |  Type  | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Graph  | Vertex  |  :heavy_check_mark: |   |
| Graph  | Edge  |  :heavy_check_mark: |   |
| Graph  | Path  |  :heavy_check_mark: |   |

### Composite Types
| Category  |  Type  | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Pair  | Pair  |  :heavy_check_mark: |   |
| Array  | int32 Array  |  :heavy_check_mark: |   |
| Array  | int64 Array  |  :heavy_check_mark: |   |
| Array  | double Array  |  :heavy_check_mark: |   |
| Array  | string Array  |  :heavy_check_mark: |   |
| Array  | pair Array  |  :heavy_check_mark: |   |
| Map  | Map  |  :x: | not planned  |

## Operators

### Clause
| Keyword |  Supported  | Desc.  |  Todo  |
|:---|:---|:---:|:---|
| MATCH | :heavy_check_mark:  |  only one Match clause is allowed |   |
| OPTIONAL MATCH | :x:  | implements as left outer join  |  planned |
| RETURN | :heavy_check_mark:  |   |   |
| WITH | :heavy_check_mark:  |  project, aggregate, distinct |   |
| WHERE | :heavy_check_mark:  |    |   |
| NOT EXIST (an edge/path) | :x: | implements as anti join | planned
| ORDER BY | :heavy_check_mark:  |    |   |
| LIMIT | :heavy_check_mark:  |    |   |

### Builtin Functions, Expression and Logical Operations
| Category  |  Operation | Supported  |  Todo  |
|:---|:---|:---:|:---|
| Aggregate | avg()  |  :heavy_check_mark: |   |
| Aggregate | min()  |  :heavy_check_mark: |   |
| Aggregate | max()  |  :heavy_check_mark: |   |
| Aggregate | count()  |  :heavy_check_mark: |   |
| Aggregate | count(distinct)  |  :heavy_check_mark: |   |
| Aggregate | sum()  |  :heavy_check_mark: |   |
| Aggregate | collect()  |  :heavy_check_mark: |   |
| Aggregate | collect(distinct)  |  :heavy_check_mark: |   |
| Logical | =, <>, >, <, >=, <= |  :heavy_check_mark: |   |
| Logical | AND, OR |  :heavy_check_mark: |   |
| Logical | NOT, IN |  :x: |  planned |
| String Match | START WITH |  :x: |  planned |
| String Match | END WITH |  :x: |  planned |
| String Match | CONTAINS |  :x: |  planned |
| String Match (Reg) | =~ |  not planned |
| Arithmetic  | +, -, *, /, % |  :heavy_check_mark: |  |
| BitOpr  | &, |, ^, ~, >>, <<, >>> |  :x: | planned |
| Branch | CASE WHEN  |  :x: | planned |
