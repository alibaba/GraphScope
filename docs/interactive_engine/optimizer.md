# A Generic Graph Optimizer for GIE

We have developed a versatile graph optimizer for the Graph Interactive Engine (GIE), capable of optimizing the execution for multiple query languages in GraphScope, including [Gremlin](./tinkerpop_eco) and [Cypher](./neo4j_eco). This optimizer is built upon the [Apache Calcite](https://calcite.apache.org/) framework, traditionally focused on relational databases, but now extended to accommodate graph databases by incorporating graph-specific operations and rules. The optimizer consists of two main components: the Rule-based Optimizer (RBO) and the Cost-based Optimizer (CBO). Like their relational counterparts, both RBO and CBO utilize pre-registered rules for transformation and path optimization, either through heuristic methods or cost estimation. However, our optimizer differs from standard relational optimizers in two key aspects:

1. **Graph-specific Rules**: Our optimizer integrates rules for specific graph operators within Graph and Relational databases, allowing for true optimization where Graph and Relational structures coexist. Relational operators directly benefit from the reuse of existing SQL rules.

2. **High-Order Statistics in CBO**: Our Cost-Based Optimizer (CBO) utilizes sophisticated statistical models grounded in graph structures, providing more accurate cardinality estimates compared to conventional optimizers. The foundational work for our CBO primarily stems from the research presented in [GlogS](https://www.usenix.org/conference/atc23/presentation/lai), which was published at ATC 2023. This research has been instrumental in enhancing the efficacy of graph pattern matching, setting a new standard for optimization in graph databases.

## Rule-based Optimizer (RBO)

| Rule Name              | Description                                                  |
|------------------------|--------------------------------------------------------------|
| FilterMatchRule        | Pushes filter conditions to graph operators in `Match`.      |
| DegreeFusionRule       | Uses pre-calculated degrees of vertices from graph storage.  |
| NotMatchToAntiJoinRule | Converts `Where Not` queries into anti-joins.                |
| FieldTrimRule          | Removes unnecessary aliases or properties.                   |

This table offers a concise overview of the existing rules for RBO. Each rule is designed to optimize graph query performance by transforming the way queries are processed and executed. Examples of how these rules conceptually affect the execution of queries are detailed as follows. Note that we use Cypher as the query language for demonstration purposes, but the rules apply to Gremlin as well.

### FilterMatchRule
This rule pushes filter conditions down to individual graph operators within a `Match` clause. For example:
```bash
Match (p1:PERSON)-[:KNOWS]->(p2:PERSON)
Where p1.id = $id1 and p2.id = $id2
Return p1, p2;
```
becomes:
```bash
Match (p1:PERSON {id: $id1})-[:KNOWS]->(p2:PERSON {id: $id2})
Return p1, p2;
```
following the push-down, splitting the `Where` clause to filter directly at the vertices p1 and p2.

### DegreeFusionRule
The DegreeFusionRule capitalizes on the fact that most graph storage systems already maintain the count of neighbors (i.e., the degree) for each vertex. For example:
```bash
Match (p1:PERSON {id: $id1})-[]->(p2)
Return p1, COUNT(p2);
```
With this rule applied, instead of individually counting each neighbor of `p1`, we can directly obtain `p1`'s degree, streamlining the process considerably. This optimization effectively utilizes pre-existing graph data structures to expedite query execution.

### NotMatchToAntiJoinRule
This rule converts queries containing `Where Not` clauses into the equivalent anti-join structures, where the anti-join
is a relational operator that returns all rows from the left table that fail to match with the right table.
Consider the following query:
```bash
Match (p1:PERSON)-[:KNOWS]->(:PERSON)-[:KNOWS]->(p2:PERSON)
Where Not (p1)-[:KNOWS]->(p2)
Return p1, p2;
```
Under this rule, the query is transformed to:
```bash
Match (p1:PERSON)-[:KNOWS]->(:PERSON)-[:KNOWS]->(p2:PERSON)
<Anti Join>
Match (p1)-[:KNOWS]->(p2)
Return p1, p2;
```
Here, `<Anti Join>` serves as a conceptual operator, used here to effectively illustrate how the original query's intent is preserved while enhancing its execution efficiency.

### FieldTrimRule
Removes unnecessary aliases or properties, reducing data processing. Consider:
```bash
Match (p1:PERSON)-[k1:KNOWS]->(p2:PERSON)-[k2:KNOWS]->(p3:PERSON)
Return p1.name;
```
Here, only `p1.name` is required in the output, making aliases like k1, k2, and properties of p2, p3 redundant.
This rule optimizes to retain only necessary data, significantly reducing the volume of the intermediate results.

## Cost-based Optimizer (CBO)
TODO
