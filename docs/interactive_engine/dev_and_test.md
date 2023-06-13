# Dev and Test

This document describes how to build and test GraphScope Interactive Engine from source code.

## Dev Environment

Here we would use a prebuilt docker image with necessary dependencies installed.

```bash
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
```

Please refer to [Dev Environment](../development/dev_guide.md#dev-environment) to find more options to get a dev environment.

## Build GIE with Vineyard Store on Local
In [GIE standalone deployment](./deployment.md), we have instructed on how to deploy GIE in a Kubenetes cluster with Vineyard store. Here, we show how to develop and test GIE with vineyard store on a local machine.

Clone the ``graphscope'' repo if you do not have it.
```bash
git clone https://github.com/alibaba/graphscope
cd graphscope
```

Now you are ready to build the GIE engine (on vineyard store) with the following command:
```bash
./gs make interactive --storage-type=vineyard
```
You can find the built artifacts in `interactive_engine/assembly/target/graphscope`.

You could install it to a location by

```bash
./gs make interactive-install --storage-type=vineyard --install-prefix /opt/graphscope
```

## Test GIE with Vineyard Store on Local
You could test the GIE engine on vineyard store with the following command:
```bash
./gs test interactive --local --storage-type=vineyard
```

This will run end2end tests, from compiling a gremlin queries to obtaining and verifying the results from the computed engine. The test includes:
  - [Tinkerpop's gremlin test](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/compiler/src/main/java/com/alibaba/graphscope/gremlin/integration/suite/standard): We replicate Tinkerpop's official test suit, which is mostly based on Tinkerpop's [modern](https://tinkerpop.apache.org/docs/3.6.2/tutorials/getting-started/)
  graph.
  - [IR pattern test](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/compiler/src/main/java/com/alibaba/graphscope/gremlin/integration/suite/pattern): In addition to Tinkerpop's official test of `match` steps, we offer extra pattern queries on modern graph.
  - [LDBC test](https://github.com/alibaba/GraphScope/blob/main/interactive_engine/compiler/src/main/java/com/alibaba/graphscope/gremlin/integration/suite/ldbc): We further test GIE against the LDBC complex workloads on the LDBC social network with the scale factor (sf) 1.
   Please refer to the [tutorial](./tutorial_ldbc_gremlin.md) for more information.

## Manually Start the GIE Services
A minimum set of GIE services includes a `frontend` to send Gremlin queries, and an `executor` (with vineyard) to execute those queries. The subsequent instructions outline the process of individually starting the `frontend` and `executor` to facilitate a more in-depth exploration of the engine.

1. First, make sure that a vineyard service is already running and a graph has been successfully loaded. Once the graph is successfully loaded into vineyard, you will obtain an `<v6d_object_id>`
for accessing the graph data.

````{hint}
If you are unsure about how to initiate a vineyard store, the subsequent instructions can assist you in creating a
vineyard store with a [modern graph](https://tinkerpop.apache.org/docs/3.6.2/tutorials/getting-started/).

```bash
export VINEYARD_IPC_SOCKET=/tmp/vineyard.sock
vineyardd --socket=${VINEYARD_IPC_SOCKET} --meta=local &
# load modern graph
export STORE_DATA_PATH=charts/gie-standalone/data  # relative to graphscope repo
vineyard-graph-loader --config charts/gie-standalone/config/v6d_modern_loader.json
```
````

2. Set the `GIE_TEST_HOME` environment variable:
```bash
export GIE_TEST_HOME=interactive_engine/assembly/target/graphscope
```

3. Configure the `$GIE_TEST_HOME/conf/executor.vineyard.properties` file:
```bash
graph.name = GRAPH_NAME
# RPC port that executor will listen on
rpc.port = 1234

# Server ID
server.id = 0

# Total server size
server.size = 1

# ip:port separated by ','
# e.g., 1.2.3.4:1234,1.2.3.5:1234
network.servers = 127.0.0.1:11234

# This worker refers to the number of threads
pegasus.worker.num = 1

graph.type = VINEYARD

# Please replace with the actual object ID of your graph
graph.vineyard.object.id: <v6d_object_id>
```

4. Start the `gaia_executor`:
```bash
$GIE_TEST_HOME/bin/gaia_executor $GIE_TEST_HOME/conf/log4rs.yml $GIE_TEST_HOME/conf/executor.vineyard.properties &
```

5. Configure the `$GIE_TEST_HOME/conf/frontend.vineyard.properties` file:
```bash
## Pegasus service config
# a.k.a. thread num
pegasus.worker.num = 1
pegasus.timeout = 240000
pegasus.batch.size = 1024
pegasus.output.capacity = 16

# executor config
# ip:port separated by ','
# e.g., 1.2.3.4:1234,1.2.3.5:1234
pegasus.hosts = localhost:1234

# graph schema path
graph.schema = /tmp/<v6d_object_id>.json

## Frontend Config
frontend.service.port = 8182

# disable authentication if username or password is not set
# auth.username = default
# auth.password = default
```

6. Start the `frontend`:
```bash
java -cp ".:$GIE_TEST_HOME/lib/*" -Djna.library.path=$GIE_TEST_HOME/lib com.alibaba.graphscope.frontend.Frontend $GIE_TEST_HOME/conf/frontend.vineyard.properties &
```

With the frontend service, you can open the gremlin console and set the endpoint to
`localhost:8182`, as given [here](./tinkerpop_gremlin.md#gremlin-console).

7. Kill the services of `vineyardd`, `gaia_executor` and `frontend`:
```
pkill -f vineyardd
pkill -f gaia_executor
pkill -f Frontend
```
