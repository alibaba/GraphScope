# GIE Graph Optimizer
The GIE Graph Optimizer is composed of RBO and CBO, similar to traditional optimizers, both RBO and CBO are based on registered Rules to perform corresponding transformations, and find the optimal execution path through heuristic or cost-estimated methods. The differences are:

1. We provide specific Rules for specific graph operators that exist in the Graph, and Relational operators can still directly reuse Sql Rules, which allows our optimizer to truly optimize the structure where Graph and Relational coexist.
2. In addition, our CBO provides high-order statistics based on the graph structure, which can provide more accurate cardinality estimates compared to traditional optimizers.

## Rules
Next, we will focus on introducing some existing Rules.
### RBO
#### FilterMatchRule
Push the Filter condition down to each graph operator in Match, for example, for the `Cypher` query:
```bash
Match (p1:PERSON)-[:KNOWS]->(p2:PERSON)
Where p1.id = $id1 and p2.id = $id2
Return p1, p2;
```
After pushing down, the filter conditions in `Where` will be broken down and pushed down to p1 and p2 respectively, equivalent to the query:
```bash
Match (p1:PERSON {id: $id1})-[:KNOWS]->(p2:PERSON {id: $id2})
Return p1, p2;
```
#### DegreeFusionRule
Calculate the degree of each vertex separately, and finally sum them up. Compared with the method of unfolding all edges and then counting, it can effectively reduce the amount of calculation. For example, for the `Gremlin` query:
```bash
g.V().out().count()
```
After Fusion, it is equivalent to the query:
```bash
g.V().out_degree().sum()
```
`out_degree` represents the operation of calculating the out-degree of the current vertex, and it is not a real operator.
#### NotMatchToAntiJoinRule
Convert where-not-sub-query to anti-join, for example, for the `Cypher` query:
```bash
Match (p1:PERSON)-[:KNOWS]->(:PERSON)-[:KNOWS]->(p2:PERSON)
Where Not (p1)-[:KNOWS]->(p2)
Return p1, p2;
```
After optimization, it becomes:
```bash
Match (p1:PERSON)-[:KNOWS]->(:PERSON)-[:KNOWS]>(p2:PERSON)
<Anti Join>
Match (p1)-[:KNOWS]->(p2)
Return p1, p2;
```
`<Anti Join>` is not a real operator, it is used here just for a better explanation of this optimization.
#### FieldTrimRule
`FieldTrimRule` can help to remove unnecessary aliases or properties from the query execution, which can reduce the amount of data that needs to be processed.

For example, consider the query:
```bash
Match (p1:PERSON)-[k1:KNOWS]->(p2:PERSON)-[k2:KNOWS]>(p3:PERSON)
Return p1.name;
```
In the end, only the 'name' attribute of p1 will be output. The aliases of k1, k2, p2, p3 do not need to be retained. Besides, except for the 'name' attribute in p1 that needs to be retained, other attributes in p1, as well as attributes in other nodes or edges involved in the query, are not necessary for the final output. The optimization to eliminate these unnecessary data retrievals can be achieved through field trim.