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
You should be able to see the port (by default: 7687) for accepting Cypher queries.  

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

## Running Queries
GraphScope Interactive seamlessly integrates with the Neo4j ecosystem. You can establish a connection to the Interactive service using Neo4j's Bolt connector and execute Cypher queries. Our implementation of Cypher queries aligns with the standards set by the [openCypher](http://www.opencypher.org/) project. For a detailed overview of the supported Cypher queries, please visit [supported_cypher](https://graphscope.io/docs/latest/interactive_engine/neo4j/supported_cypher).

### Connect to the Service
Connect to the GraphScope Interactive Service using the cypher-shell from Neo4j:

```bash
# Get Cypher-shell from: https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.22.zip
wget -O cypher-shell-4.4.22.zip https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.22.zip
unzip cypher-shell-4.4.22.zip
./cypher-shell/cypher-shell -a neo4j://localhost:7687
```

User name and password are not needed for now, while such authentication will be provided in a future version. 

### Submit Queries
Once connected, you can start submitting your queries:

```cypher
@neo4j> MATCH(v:person { name: "peter"}) RETURN v.age;
```
Then you can get the age of `peter`.
```txt
+-----+
| age |
+-----+
| 35  |
+-----+
```

Note: Cypher queries submitted to GraphScope Interactive are compiled into a dynamic library for execution. While the initial compilation might take some time, a future version will provide caching the library to ensure faster execution for subsequent uses (of the **same** query).