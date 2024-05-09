# Getting Started

Welcome to GraphScope Interactive! This guide will walk you through the process of setting up and running your first queries with our system.

## Preparation

Make sure `GraphScope Interactive` is installed before proceeding on. If not, please follow [installation](./installation) to install the latest release.

- **Administrative tool**: GraphScope Interactive offers an administrative tool of `bin/gs interactive` to help manage the Interactive service. For an in-depth guide on how to use this tool, please visit the page of [administrative tool](./tools/admin_tool). 

- **Graph Data**: By default, GraphScope Interactive uses Tinkerpop's [modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) to get you started. However, if you wish to configure your own graph and load your data, you can refer to the page of [using custom graph_data](./custom_graph) for detailed steps.
  
- **Configurations**: The service can be configured using the file located at `conf/gs_interactive_conf.yaml`. For a detailed breakdown of all configurable items, please refer to the page of [configurations](./interactive_conf). 

- **Stored Procedures**: GraphScope Interactive allows users to register stored procedures from both Cypher and C++ files. For more information on this, please refer to the page for [stored procedures](./stored_procedures).
  

## Manage Interactive Service
We encapsulate the internal implementation of GraphScope Interactive through a Docker container, ensuring compatibility across different runtime environments.

### Initialize the Service
This step will pull the specified version of the GraphScope Interactive image and create the container. 

```bash
bin/gs_interactive init -c conf/interactive.yaml
```

Note: The `yaml` file is used to specify the docker-related configurations such as container/host mappings of port and volume.


### Start the Service
To start the Interactive service, provide the raw graph data and meta info. For instance, you can start the service on the default modern graph with the following command:

```bash
bin/gs_interactive service start
```
You should be able to see a message telling you what the `Gremlin/Cypher` endpoint that you should connect to.
### Stop the Service
To stop the Interactive service, simple type in the following command:

```bash
bin/gs_interactive service stop
```
### Restart the Service
To apply changes made in the [configurations](./interactive_conf) or to enable/disable specific [stored procedures](./stored_procedures), it's typically required to restart the service:

```bash
bin/gs_interactive service restart
```

### Check Service Status
To check the status of the Interactive service, just type in:

```bash
bin/gs_interactive service status
```
You should be able to see the current status of the service, the graph that is running and the port (by default: 7687) for accepting Cypher queries.   


### Destroy the Service
Execute the command below to terminate the service. Please be aware that this action will erase all the data you've generated and is irreversible. Please proceed with caution.

```bash
bin/gs_interactive destroy
```

## Running Cypher Queries
GraphScope Interactive seamlessly integrates with the Neo4j ecosystem. You can establish a connection to the Interactive service using Neo4j's Bolt connector and execute Cypher queries. Our implementation of Cypher queries aligns with the standards set by the [openCypher](http://www.opencypher.org/) project. For a detailed overview of the supported Cypher queries, please visit [supported_cypher](https://graphscope.io/docs/latest/interactive_engine/neo4j/supported_cypher).

Follow the instructions in [Connect-to-cypher-service](../../interactive_engine/neo4j/cypher_sdk) to connect to the Cypher service using either the Python client or cypher-shell.


Note: Cypher queries submitted to GraphScope Interactive are compiled into a dynamic library for execution. While the initial compilation might take some time, the execution for subsequent uses (of the **same** query) will be much faster since it is cached by Interactive.

## Running Gremlin Queries

GraphScope Interactive supports the property graph model and Gremlin traversal language defined by Apache TinkerPop,
Please refer to the following link to connect to the Tinkerpop Gremlin service provided by GraphScope Interactive: [Connect-to-gremlin-service](../../interactive_engine/tinkerpop/tinkerpop_gremlin)