# Turning Cypher Queries into Stored Procedures with GraphScope Interactive

GraphScope Interactive offers a seamless way for users to transform Cypher queries into stored procedures, eliminating the need for intricate C++ programming. Drawing inspiration from Neo4j, we empower users to craft a query skeleton, incorporating runtime parameters denoted by `$param_name`.


## Define a Cypher Stored Procedure

In GraphScope Interactive, a Cypher Stored Procedure is defined via a `yaml` file.

```yaml
name: <procedure_name>
description: <description about this stored procedure>
query: <The cypher query string>
type: cypher
```

Here comes the detail explanation for each field.

- name(required): A unique identifier for a stored procedure, which you will need when calling the stored procedure from either the Interactive SDK or neo4j-native tools. The uniqueness is within the context of a graph, which means you can define two stored procedure with the same name in two different graph. But when calling procedure from client, make sure the Interactive instance is running on the graph you desired.
- description(optional): A string which help you remember and illustrate the use of this procedure. If not given, a default description will be assigned.
- query(required): The cypher query string which defines the logic of this procedure. Template cypher query is supported, use can denote the runtime parameters as `$param_name`, and assign the param with actual values till calling the stored procedure.
- type(required): Make sure is set to `cypher`.


## Template the Stored Procedure

With params introduced in cypher query, a template stored procedure could be crafted, and used flexibly. For example, for a simple cypher query 

```cypher
MATCH(n: person) WHERE n.id == 123 RETURN n.name;
```

we can parameterized the input id as a parameter
```cypher
MATCH(n: person) WHERE n.id == $personId RETURN n.name;
```

And define it as a stored procedure.

```yaml
name: get_person_name
description: A template stored procedure to get person's name from id.
query: "MATCH(n: person) WHERE n.id == $personId RETURN n.name;"
type: cypher
```

## Creating Stored Procedure and Calling 

Please refer to the [StoredProcedure Documentation](../../stored_procedures.md) for creating stored procedures with `gsctl` and calling stored procedures.

Refer to [Java SDK Documentation](../java/java_sdk.md#create-a-stored-procedure) and [Python SDK Documentation](../python/python_sdk.md#create-a-stored-procedure) for creating stored procedures with SDKs.