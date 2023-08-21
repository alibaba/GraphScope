# Getting Started

Welcome to GraphScope Interactive! This guide will walk you through the process of setting up and running your first queries with our system.

## Prerequisites
- Docker Configuration: Ensure you have Docker installed and configured on a Mac or Linux machine. If you haven't installed Docker yet, you can find the installation guide [here](https://docs.docker.com/get-docker/).

- Download Required Files: Download the compressed file containing Docker configuration files, example data, and other necessary resources from [here](TODO). Extract the contents to a suitable location on your machine:
  ```bash
  tar xvf graphscope_interactive.tar.gz
  cd graphscope_interactive
  export GS_INTERACTIVE_HOME=`pwd`
  ```
- Administrative tool: GraphScope Interactive offers an administrative tool located at $GS_INTERACTIVE_HOME/bin/gs interactive to help manage the Interactive service. For an in-depth guide on how to use this tool, please visit the page of [adminstrative tool](./tools/gs_interactive_admin.md). TODO (longbin): Do we need to specify an extra layer of admin like: `gs interactive admin`?

- Graph Data: By default, GraphScope Interactive uses Tinkerpop's [modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/) to get you started. However, if you wish to configure your own graph and load your data, you can refer to the page of [data import](./data_import.md) for detailed steps.

- Stored Procedures: GraphScope Interactive allows users to register stored procedures from both Cypher and C++ files. For more information on this, please refer to the page for [stored procedures](TODO).
  

## Manage Interactive Service
We encapsulate the internal implementation of GraphScope Interactive through a Docker container, ensuring compatibility across different runtime environments.

### Initialize the Service
This step will pull the specified version of the GraphScope Interactive image and create the container. 

```bash
$GS_INTERACTIVE_HOME/bin/gs interactive init -v v0.0.1
```

Note: `-v` (`--version`) option is used to specify the version of the docker image, if it is not specified, the latest version will be used by default.

### Start the Service
To start the Interactive service, provide the raw graph data and meta info. For instance, you can start the service on the sample graph with the following command:

```bash
$GS_INTERACTIVE_HOME/bin/gs interactive start -n modern -b ./examples/modern_graph.yaml
```

Note: 

- `-n` (`--name`) option is used to specify the graph to use, if not specified, the default modern graph will be used.
- `-b` (`--bulk_loading`) is used to specify the data to be loaded into GraphScope Interactive, while user can ignore the `-b` option and load data after starting the service, see the [data_loading](./data_loading.md) page for more details.
- The system can be configured using the file located at `$GS_INTERACTIVE_HOME/conf/gs_interactive.yaml`. For a detailed breakdown of all configurable items, please refer to the page of [configurations](./configurations.md). (TODO: add configurations page).

### Stop the Service
TODO 

### Remove the Service
TODO

## Running Queries
GraphScope Interactive seamlessly integrates with the Neo4j ecosystem. You can establish a connection to the Interactive service using Neo4j's Bolt connector and execute Cypher queries. Our implementation of Cypher queries aligns with the standards set by the [openCypher](http://www.opencypher.org/) project. For a detailed overview of the supported Cypher queries, please visit this [page](https://graphscope.io/docs/latest/interactive_engine/neo4j/supported_cypher).

### Connect to the Service
Connect to the GraphScope Interactive Service using the cypher-shell from Neo4j:

```bash
# Get Cypher-shell from: https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.22.zip
unzip cypher-shell-4.4.22.zip
./cypher-shell/cypher-shell -a neo4j://localhost:7687
```

Note: 
- User name and password are not needed for now, while such authentication will be provided in a future version. 

### Submit Queries
Once connected, you can start submitting your queries:

```cypher
@neo4j> Match (n) Return n Limit 10;
```

Note: Cypher queries submitted to GraphScope Interactive are compiled into a dynamic library for execution. While the initial compilation might take some time, the library is cached to ensure faster execution for subsequent uses (of the **same** query).

## Configurations
Users have the flexibility to configure GraphScope Interactive using a `yaml` configuration file. A sample of this can be found at `$GS_INTERACTIVE_HOME/conf/gs_interactive.yaml`. For a comprehensive breakdown of all configurable elements, please refer to the page of [configurations](./configurations.md).