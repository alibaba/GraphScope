# Stored Procedures

Stored procedures in GraphScope Interactive offer a powerful way to encapsulate and reuse complex graph operations. This document provides a guide on how to compile, enable, and manage these procedures. We will take movies graph for example.

```{note}
Before executing the following code, please ensure that you are in the context of the builtin graph `gs_interactive_default_graph`: `gsctl use GRAPH gs_interactive_default_graph`. 
```

## Define a Stored Procedure

To compile a stored procedure, first define it with a YAML file named `procedure.yaml`, which defines a stored procedure that searches for `softwares` created by the person with the provided name. The person's name has been parameterized as `$personName`.


```yaml
name: test_procedure
description: "Ths is a test procedure"
query: 'MATCH (p: person)-[c:created]->(s: software) where p.name = $personName RETURN s.id as softwareId, s.name as softwareName;'
type: cypher
```

Note:
- `name` is required.
- `description` is optional.
- The string in `query` field could be either `cypher` query or `c++` code. For comprehensive guidelines on crafting stored procedures in GraphScope Interactive using both Cypher and C++, refer to the [Cypher procedure](./development/stored_procedure/cypher_procedure.md) and [C++ procedure](./development/stored_procedure/cpp_procedure.md) documentation.
- When compiling from Cypher code, the optimization rules defined under [`compiler.planner`](./configuration) will be taken into account to generate a more efficient program.

For more information on defining a Cypher stored procedure, please refer to [Cypher Stored Procedure](./development/stored_procedure/cypher_procedure.md).

## Create a Stored Procedure

Then create the procedure with `gsctl`:

```bash
gsctl create storedproc -f ./procedure.yaml
```

This will invoke the compilation procedure to convert the cypher query to a physical plan, then generate C++ code and compile it, so it may take some time.


Restart the service is **necessary** to activate the stored procedures:

```bash
gsctl service restart
```

## Delete a Stored Procedures

To delete a single stored procedures, simply using:

```bash
gsctl delete storedproc test_procedure
```

## Viewing Stored Procedures

To view a single stored procedure, 

```bash
gsctl desc storedproc test_procedure
```

Or, show all valid procedures in cypher shell.
```bash
@neo4j> Show Procedures;
```

## Querying Stored Procedures 

#### Call the Stored Procedure via Interactive SDK

You can call the stored procedure via Interactive Python SDK. (Make sure environment variables are set correctly, see [Deploy Interactive](./installation.md#install-and-deploy-interactive)).

```bash
export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:7777
export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:10000
export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:7687
```

```{note}
If you have customized the ports when deploying Interactive, remember to replace the default ports with your customized ports.
```

```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
with driver.getNeo4jSession() as session:
    result = session.run('CALL test_procedure("marko") YIELD *;')
    for record in result:
        print(record)
```

#### Call the Stored Procedure via Neo4j-native Tools

You can also call the stored procedure via neo4j-native tools, like `cypher-shell`, `neo4j-driver`. Please refer to [this document](../../interactive_engine/neo4j/cypher_sdk) for connecting to cypher service.


```bash
./cypher-shell -a ${INTERACTIVE_CYPHER_ENDPOINT}
```

```cypher
CALL test_procedure("marko") YIELD *;
```
