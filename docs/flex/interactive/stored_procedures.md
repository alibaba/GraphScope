# Stored Procedures

Stored procedures in GraphScope Interactive offer a powerful way to encapsulate and reuse complex graph operations. This document provides a guide on how to compile, enable, and manage these procedures. We will take movies graph for example.

> Before executing the following code, please ensure that you are in the context of the graph `movies`: `gsctl use GRAPH movies`.

## Compiling a Stored Procedure

To compile a stored procedure, first define it with a yaml file `procedure.yaml`

```yaml
name: test_procedure
description: "Ths is a test procedure"
query: "MATCH (n) RETURN COUNT(n);"
type: cypher
```

Then create the procedure with `gsctl`:

```bash
gsctl create storedproc -f ./procedure.yaml
```

This will invoke the compilation procedure to convert the cypher query to a physical plan, then generate C++ code and compile it, so it may take some time.


Note:
- `name` is required.
- `description` is optional.
- The string in `query` field could be either `cypher` query or `c++` code. For comprehensive guidelines on crafting stored procedures in GraphScope Interactive using both Cypher and C++, refer to the Cypher procedure and C++ procedure documentation.
- When compiling from Cypher code, the optimization rules defined under [`compiler.planner`](./configuration) will be taken into account to generate a more efficient program.


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
gsctl desc test_procedure
```

(TODO, currently not supprted)To view all stored procedures of the given graph after starting the service, execute:

```bash
gsctl list storedproc
```

This command provides a list of all active (enabled) stored procedures with their metadata including names and descriptions in your GraphScope Interactive instance.

Or, in the Cypher shell, run:
```bash
@neo4j> Show Procedures;
```

## Querying Stored Procedures 

#### Call the Stored Procedure via Interactive SDK

You can call the stored procedure via Interactive Python SDK. (Make sure environment variables are set correctly, see [above session](#deploy-in-local-mode)).

```bash
export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:{admin_port}
export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:{storedproc_port}
export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:{cypher_port}
export INTERACTIVE_GREMLIN_ENDPOINT=ws://127.0.0.1:{gremlin_port}/gremlin
```

```python
from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
with driver.getNeo4jSession() as session:
    result = session.run("CALL test_procedure() YIELD *;")
    for record in result:
        print(record)
```

#### Call the Stored Procedure via Neo4j Ecosystem

You can also call the stored procedure via neo4j-native tools, like `cypher-shell`, `neo4j-driver`. Please refer to [this document](../../interactive_engine/neo4j/cypher_sdk) for connecting to cypher service.


```bash
./cypher-shell -a ${INTERACTIVE_CYPHER_ENDPOINT}
```

```cypher
CALL test_procedure() YIELD *;
```

Note that you can not call stored procedure via `Tinkpop Gremlin` tools, since stored procedure is not supported in `Gremlin`.
