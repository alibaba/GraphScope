# Getting Started

Welcome to GraphScope Interactive! This guide will walk you through the process of setting up and running your first queries with our system.

## Preparation

Make sure `GraphScope Interactive` is installed before proceeding on. If not, please follow [installation](./installation) to install the latest release.

- **Administrative tool**: GraphScope Interactive offers an administrative tool of `gsctl` to help manage the Interactive service. For an in-depth guide on how to use this tool, please visit the page of [administrative tool]() (TODO: add documentation for gsctl).

- **Graph Data**: By default, GraphScope Interactive uses Tinkerpop's [modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) to get you started. However, if you wish to configure your own graph and load your data, you can refer to the page of [using custom graph_data](./custom_graph) for detailed steps.
  
- **Configurations**: The service can be configured using a yaml file. For a detailed breakdown of all configurable items, please refer to the page of [configurations](./configuration). 

- **Stored Procedures**: GraphScope Interactive allows users to register stored procedures from both Cypher and C++ files. For more information on this, please refer to the page for [stored procedures](./stored_procedures).
  

## Install Interactive

We offer a command line tool called `gsctl`, which allows you to install, start, and manage Interactive services.

The Interactive service is deployed in a docker container, ensuring compatibility across different runtime environments.

```{note}
Docker is required.
```

```bash
pip3 install gsctl
```


## Deploy in Local Mode

You can deploy the Interactive service locally with the following command.
```bash
gsctl instance deploy --type interactive 
# Or you can customize the port number
gsctl instance deploy --type interactive --coordinator-port 8081 --admin-port 7778 --cypher-port 7688 --storedproc-port 10001 --gremlin-port 8183
```

```{note}
1. Beside from the interactive server, a coordinator server is also launched. The coordinator server acts like the `ApiServer` for k8s,
allowing users to interact with the GraphScope platform through a simplified and consistent set of API.
2. Gremlin service is disabled by default, To enable it, try specifying the Gremlin port.
```

The following message will display on your screen to inform you about the available services:

```txt
Coordinator is listening on {coordinator_port} port, you can connect to coordinator by:
    gsctl connect --coordinator-endpoint http://127.0.0.1:{coordinator_port}

Interactive service is ready, you can connect to the interactive service with interactive sdk:
Interactive Admin service is listening at
    http://127.0.0.1{admin_port},
You can connect to admin service with Interactive SDK, with following environment variables declared.

############################################################################################
    export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:{admin_port}
    export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:{storedproc_port}
    export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:{cypher_port}
    export INTERACTIVE_GREMLIN_ENDPOINT=ws://127.0.0.1:{gremlin_port}/gremlin
############################################################################################
```

Remember to copy the environment exporting commands and execute them.

You could connect the `coordinator` via `gsctl`. 

```bash
gsctl connect --coordinator-endpoint http://127.0.0.1:8081
```

For the detail usage of gsctl, please refer to the documentation of `gsctl`(TODO). In this document, 
we will give you a quick guide of using `gsctl` to mange the Interactive Service.

## Create a New Graph.

Although a builtin graph named `modern_graph` is already serving after the service is launched, you may want to create you own graph.
First you need to define the vertex types and edge types of your graph, i.e. here is a sample definition of a graph containing only one kind of vertex,
`person`, and one edge type `knows` between `person` and `person`. Save the file to disk with name `test_graph.yaml`. 

```yaml
name: test_graph
description: "This is a test graph"
schema:
  vertex_types:
    - type_name: person
      properties:
        - property_name: id
          property_type:
            primitive_type: DT_SIGNED_INT64
        - property_name: name
          property_type:
            string:
              long_text: ""
        - property_name: age
          property_type:
            primitive_type: DT_SIGNED_INT32
      primary_keys:
        - id
  edge_types:
    - type_name: knows
      vertex_type_pair_relations:
        - source_vertex: person
          destination_vertex: person
          relation: MANY_TO_MANY
      properties:
        - property_name: weight
          property_type:
            primitive_type: DT_DOUBLE
```

Please refer to [DataModel](./data_model.md) for more details about defining a graph.

Now you can create a new graph with gsctl

```bash
gsctl create graph -f ./test_graph.yaml
```

## Import Data

To import data to the new graph, another configuration file is needed, let's say `import.yaml`. 
Each vertex/edge type need at least one input for bulk loading. 
In the following example, we will import data to the new graph from local file
`person.csv` and `person_knows_person.csv`. You can download the files from [GitHub](https://github.com/alibaba/GraphScope/tree/main/flex/interactive/examples/modern_graph).
Remember to replace `@/path/to/person.csv` and `@/path/to/person_knows_person.csv` with the actual path to files.

```{note}
`@` means the file is a local file and need to be uploaded.
```

```yaml
vertex_mappings:
  - type_name: person
    inputs:
      - "@/path/to/person.csv"
    column_mappings:
      - column:
          index: 0
          name: id
        property: id
      - column:
          index: 1
          name: name
        property: name
      - column:
          index: 2
          name: age
        property: age
edge_mappings:
  - type_triplet:
      edge: knows
      source_vertex: person
      destination_vertex: person
    inputs:
      - "@/path/to/person_knows_person.csv"
    source_vertex_mappings:
      - column:
          index: 0
          name: person.id
        property: id
    destination_vertex_mappings:
      - column:
          index: 1
          name: person.id
        property: id
    column_mappings:
      - column:
          index: 2
          name: weight
        property: weight
```

Now create a data bind via `gsctl`. 

```bash
gsctl create datasource -f ./import.yaml -g test_graph
```

So far, we have only created the dataource, a job config `job_config.yaml` is also needed to data import.

```yaml
loading_config:
  import_option: overwrite
  format:
    type: csv
    metadata:
      delimiter: "|"
      header_row: "true"

vertices:
  - type_name: person

edges:
  - type_name: knows
    source_vertex: person
    destination_vertex: person
```

Now create a bulk loading job via `gsctl`.

```bash
gsctl create loaderjob -f ./job_config.yaml -g test_graph
```

A job id will be returned, and you can check the loading progress/result by
```bash
gsctl desc job xxx
```


## Create a Stored Procedure

Currently Interactive is only able to provide query service on one graph at a time. So we need to switch the query service by

```bash
gsctl use GRAPH test_graph
```

Now as query service is running on `test_graph`, we proceed on to create a stored procedure by define a procedure with a yaml configuration: `procedure.yaml`.

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

### Start Service on the New Graph

The stored procedure will not be able to serve requests until we restart the service.

```bash
gsctl service restart 
```

## Call the Stored Procedure

You have two options to call the stored procedure, one is through Interactive SDK, and the other is through the native tools of Cypher.

### Call the Stored Procedure via Interactive SDK

You can call the stored procedure via Interactive Python SDK. (Make sure environment variables are set correctly, see [above session](#deploy-in-local-mode)).

```bash
export INTERACTIVE_ADMIN_ENDPOINT=http://127.0.0.1:{admin_port}
export INTERACTIVE_STORED_PROC_ENDPOINT=http://127.0.0.1:{storedproc_port}
export INTERACTIVE_CYPHER_ENDPOINT=neo4j://127.0.0.1:{cypher_port}
export INTERACTIVE_GREMLIN_ENDPOINT=ws://127.0.0.1:{gremlin_port}/gremlin
```

```python
import os

from gs_interactive.client.driver import Driver
from gs_interactive.client.session import Session
from gs_interactive.models import *

driver = Driver()
with driver.getNeo4jSession() as session:
    result = session.run("CALL test_procedure() YIELD *;")
    for record in result:
        print(record)
```

### Call the Stored Procedure via Neo4j Ecosystem

You can also call the stored procedure via neo4j-native tools, like `cypher-shell`, `neo4j-driver`. Please refer to [this document](../../interactive_engine/neo4j/cypher_sdk) for connecting to cypher service.


```bash
./cypher-shell -a ${INTERACTIVE_CYPHER_ENDPOINT}
```

```cypher
CALL test_procedure() YIELD *;
```

Note that you can not call stored procedure via `Tinkpop Gremlin` tools, since stored procedure is not supported in `Gremlin`.


## Submit Adhoc Queries

Both `cypher` and `gremlin` queries are supported by Interactive.

### Running Cypher Queries

GraphScope Interactive seamlessly integrates with the Neo4j ecosystem. You can establish a connection to the Interactive service using Neo4j's Bolt connector and execute Cypher queries. Our implementation of Cypher queries aligns with the standards set by the [openCypher](http://www.opencypher.org/) project. For a detailed overview of the supported Cypher queries, please visit [supported_cypher](https://graphscope.io/docs/latest/interactive_engine/neo4j/supported_cypher).

Follow the instructions in [Connect-to-cypher-service](../../interactive_engine/neo4j/cypher_sdk) to connect to the Cypher service using either the Python client or cypher-shell.


Note: Cypher queries submitted to GraphScope Interactive are compiled into a dynamic library for execution. While the initial compilation might take some time, the execution for subsequent uses (of the **same** query) will be much faster since it is cached by Interactive.

### Running Gremlin Queries

GraphScope Interactive supports the property graph model and Gremlin traversal language defined by Apache TinkerPop,
Please refer to the following link to connect to the Tinkerpop Gremlin service provided by GraphScope Interactive: [Connect-to-gremlin-service](../../interactive_engine/tinkerpop/tinkerpop_gremlin)

## Close the connection

If you want to disconnect to coordinator, just type

```bash
gsctl close
```

## Close the Interactive Instance

If you want to shutdown and uninstall the Interactive instance,

```bash
gsctl instance destroy --type interactive
```

This will remove all the graph and data for the Interactive instance.