# Turning Cypher Queries into Stored Procedures with GraphScope Interactive

GraphScope Interactive offers a seamless way for users to transform Cypher queries into stored procedures, eliminating the need for intricate C++ programming. Drawing inspiration from Neo4j, we empower users to craft a query skeleton, incorporating runtime parameters denoted by `$param_name`.


## Crafting a Cypher Stored Procedure
For optimal organization and clarity, we mandate that each Cypher stored procedure be encapsulated within a distinct file. This file should exclusively house one Cypher query, although line breaks can be employed for readability.

For instance, if you aim to retrieve the count of nodes labeled "person" with a personId that matches a runtime-specified value, you can draft the query as shown below:

```cypher
MATCH (p :PERSON {id: $personId })
RETURN p.firstName, p.lastName
```

## Compiling and Enabling the Stored Procedure
To compile the aforementioned Cypher query and activate it as a stored procedure, utilize the `gsctl create procedure` command:

Before proceeding, make sure you are within the context of your desired graph, for which we are creating the stored procedure. 

```{note}
It's crucial to note that each stored procedure is intrinsically linked to a specific graph. This graph must pre-exist.
```

```bash
gsctl use GRAPH movies
```

A stored procedure should be defined via a yaml file, for example `procedure.yaml`

```yaml
name: test_procedure
description: "Ths is a test procedure"
query: "MATCH (p :PERSON {id: $personId }) RETURN p.firstName, p.lastName"
type: cypher
```

## Invoking the Cypher Stored Procedure
Once the stored procedure is activated, a service restart is imperative:

```bash
gsctl service restart
```

Subsequently, from the cypher-shell, you can invoke the stored procedure as follows:

```cypher
CALL query1(1L) YIELD *;
```
 
In conclusion, GraphScope Interactive's ability to effortlessly convert Cypher queries into stored procedures streamlines and simplifies the user experience, making graph analytics more accessible and efficient.