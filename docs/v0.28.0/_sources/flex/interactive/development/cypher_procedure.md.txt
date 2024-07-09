# Turning Cypher Queries into Stored Procedures with GraphScope Interactive

GraphScope Interactive offers a seamless way for users to transform Cypher queries into stored procedures, eliminating the need for intricate C++ programming. Drawing inspiration from Neo4j, we empower users to craft a query skeleton, incorporating runtime parameters denoted by `$param_name`.


## Crafting a Cypher Stored Procedure
For optimal organization and clarity, we mandate that each Cypher stored procedure be encapsulated within a distinct file. This file should exclusively house one Cypher query, although line breaks can be employed for readability.

For instance, if you aim to retrieve the count of nodes labeled "person" with a personId that matches a runtime-specified value, you can draft the query as shown below and save it as `query1.cypher`:

```cypher
MATCH (p :PERSON {id: $personId })
RETURN p.firstName, p.lastName
```

## Compiling and Enabling the Stored Procedure
To compile the aforementioned Cypher query and activate it as a stored procedure, utilize the `gs_interactive procedure` command:

```bash
bin/gs_interactive procedure compile 
	-g modern  
	-n "query1" \
	-d "a sample test query" \
	-i ./examples/modern/query1.cypher
```

It's crucial to note that each stored procedure is intrinsically linked to a specific graph, as indicated by the `-g` option. This graph must pre-exist. In the absence of a specified graph, the system will prompt you to utilize the default graph.



## Invoking the Cypher Stored Procedure
Once the stored procedure is activated, a service restart is imperative:

```bash
./bin/gs_interactive service restart
```

Subsequently, from the cypher-shell, you can invoke the stored procedure as follows:

```cypher
CALL query1(1L) YIELD *;
```
 
In conclusion, GraphScope Interactive's ability to effortlessly convert Cypher queries into stored procedures streamlines and simplifies the user experience, making graph analytics more accessible and efficient.