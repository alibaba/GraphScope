# Getting Started

Welcome to GraphScope Interactive! This guide will walk you through the process of setting up and running your first queries with our system.

## Preparation

Make sure `GraphScope Interactive` is installed before proceeding on. If not, please follow [installation](./installation) to install the latest release.

- **Administrative tool**: GraphScope Interactive offers an administrative tool of `gsctl` to help manage the Interactive service. For an in-depth guide on how to use this tool, please visit the page of [administrative tool]() (TODO: add documentation for gsctl).

- **Graph Data**: By default, GraphScope Interactive uses Tinkerpop's [modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) to get you started. However, if you wish to configure your own graph and load your data, you can refer to the page of [using custom graph_data](./custom_graph) for detailed steps.
  
- **Configurations**: The service can be configured using a yaml file. For a detailed breakdown of all configurable items, please refer to the page of [configurations](./configuration). 

- **Stored Procedures**: GraphScope Interactive allows users to register stored procedures from both Cypher and C++ files. For more information on this, please refer to the page for [stored procedures](./stored_procedures).
  

## Install Interactive

See [Installation Guide](./installation.md) for instructions on how to install and deploy Interactive.


## Connect to Interactive Service

You could connect the `coordinator` via `gsctl`. 

```bash
gsctl connect --coordinator-endpoint http://127.0.0.1:8080
# change the port number if you have customized the coordinator port.
```


## Check Service Status

After connecting to the Interactive Service, you can now view what we have initially for you.

```bash
gsctl ls -l
```

Actually, a builtin graph is provided with name `gs_interactive_default_graph`. Now you can switch to the graph context:

```bash
gsctl use GRAPH gs_interactive_default_graph
gsctl service status # show current service status
```

As seen from the output, the Interactive service is already running on the built-in graph.

## Create a Stored Procedure

We proceed to create a stored procedure by defining a procedure with a YAML configuration: `procedure.yaml`.

```yaml
name: test_procedure
description: "Ths is a test procedure"
query: "MATCH (n) RETURN COUNT(n);"
type: cypher
```

and create it via 

```bash
gsctl create storedproc -f ./procedure.yaml
```

This step may take a while, since code generation and compilation is invoked.
Finally, the id of the stored procedure(currently the name of the procedure) is returned, 
you can display the detail of the stored procedure with the following command.

```bash
gsctl desc storedproc test_procedure
```

You may notice that the value of`runnable` field is `false`, we need to restart service to enable it.

### Restart the service

The stored procedure will not be able to serve requests until we restart the service.

```bash
gsctl service restart 
```

After the service is restarted, check the runnable field; it should be set to true.

```bash
gsctl desc storedproc test_procedure
```

## Call the Stored Procedure

You have two options to call the stored procedure, one is through Interactive SDK, and the other is through the native tools of Cypher.

### Call the Stored Procedure via Interactive SDK

You can call the stored procedure via Interactive Python SDK. (Make sure environment variables are set correctly, see [above session](#deploy-in-local-mode)).

```bash
export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:7777
export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:10000
export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:7687
```

```{note}
If you have customized the ports when deploying Interactive, remember to replace the default ports with your customized ports.
```

Install Interactive python SDK

```bash
pip3 install gs_interactive
```

and try to call the stored procedure.

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

### Call the Stored Procedure via Neo4j-native Tools

You can also call the stored procedure via neo4j-native tools, like `cypher-shell`, `neo4j-driver`. Please refer to [this document](../../interactive_engine/neo4j/cypher_sdk) for connecting to cypher service.


```bash
./cypher-shell -a ${INTERACTIVE_CYPHER_ENDPOINT}
```

```cypher
CALL test_procedure() YIELD *;
```


## Submit Cypher Queries

GraphScope Interactive seamlessly integrates with the Neo4j ecosystem. You can establish a connection to the Interactive service using Neo4j's Bolt connector and execute Cypher queries. Our implementation of Cypher queries aligns with the standards set by the [openCypher](http://www.opencypher.org/) project. For a detailed overview of the supported Cypher queries, please visit [supported_cypher](https://graphscope.io/docs/latest/interactive_engine/neo4j/supported_cypher).

Follow the instructions in [Connect-to-cypher-service](../../interactive_engine/neo4j/cypher_sdk) to connect to the Cypher service using either the Python client or cypher-shell.


Note: Cypher queries submitted to GraphScope Interactive are compiled into a dynamic library for execution. While the initial compilation might take some time, the execution for subsequent uses (of the **same** query) will be much faster since it is cached by Interactive.

## Close the connection

If you want to disconnect to coordinator, just type

```bash
gsctl close
```

## Destroy the Interactive Instance

If you want to shutdown and uninstall the Interactive instance,

```bash
gsctl instance destroy --type interactive
```

**This will remove all the graph and data for the Interactive instance.**