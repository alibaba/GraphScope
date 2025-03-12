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


## Submit Cypher Queries

GraphScope Interactive seamlessly integrates with the Neo4j ecosystem. You can establish a connection to the Interactive service using Neo4j's Bolt connector and execute Cypher queries. Our implementation of Cypher queries aligns with the standards set by the [openCypher](http://www.opencypher.org/) project. For a detailed overview of the supported Cypher queries, please visit [supported_cypher](https://graphscope.io/docs/latest/interactive_engine/neo4j/supported_cypher).

Follow the instructions in [Connect-to-cypher-service](../../interactive_engine/neo4j/cypher_sdk) to connect to the Cypher service using either the Python client or cypher-shell. 

To submit a simple query using `cypher-shell`:

#### Download `cypher-shell`

```bash
wget https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.19.zip
unzip cypher-shell-4.4.19.zip && cd cypher-shell
```

#### Connect to the Service

```bash
./cypher-shell
# or -a neo4j://localhost:<port> to connect to the customized port
```

#### Run a Simple Query

```bash
@neo4j> MATCH (n) RETURN n LIMIT 10;
```

You could also make use of Interactive SDKs,[Java SDK](./development/java/java_sdk.md) or [Python SDK](./development/python/, to connect to the Interactive service using the python_sdk.md) to submit queries.

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