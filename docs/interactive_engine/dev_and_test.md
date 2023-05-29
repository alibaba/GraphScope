# Dev and Test

This document describes how to build and test GraphScope Interactive Engine from source code.

## Dev Environment

Here we would use a prebuilt docker image with necessary dependencies installed.

```bash
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
```

Please refer to [Dev Environment](../development/dev_guide.md#dev-environment) to find more options to get a dev environment.

## Build and Test GIE with Local Experimental Store
Clone a repo if needed.
```bash
git clone https://github.com/alibaba/graphscope
cd graphscope
```
Now you are ready to build the code with a local ``experimental'' store that is included only for local testing purpose.
```bash
./gs make interactive --storage-type=experimental
```

You then could locally test the GIE engine with a single command:
```bash
./gs test interactive --local --storage-type=experimental
```

Recall that in [GIE](./design_of_gie.md), a gremlin query will be firstly parsed to an IR logical plan by the compiler, and then into a physical plan,
which will be further assembled into a job that can be executed in the
computing engine (Pegasus). There mainly include the following three parts of test:

- GIE compiler unit test: This part of test goes through the Java codebase of compiler in `interactive_engine/compiler`,
which will verifies the correctness of generating the IR logical plan from some [gremlin steps](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/compiler/src/test/java/com/alibaba/graphscope/gremlin).
- GIE IR unit test: This part of test goes through the Rust codebase of IR layer in `interactive_engine/executor/ir`, and runs `cargo test`
for each rust package, mainly including:
  - `core`: Processing an IR logical plan into a physical plan.
  - `runtime`: Assembling a physical plan into an executable job.
- Integration test: An e2e test, from compiling a gremlin queries to obtaining the results from the
computed engine. The test includes:
  - [Tinkerpop's gremlin test](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/compiler/src/main/java/com/alibaba/graphscope/gremlin/integration/suite/standard): We replicate Tinkerpop's official test suit, which is mostly based on Tinkerpop's [modern](https://tinkerpop.apache.org/docs/3.6.2/tutorials/getting-started/)
  graph.
  - [IR pattern test](https://github.com/alibaba/GraphScope/tree/main/interactive_engine/compiler/src/main/java/com/alibaba/graphscope/gremlin/integration/suite/pattern): In addition to Tinkerpop's official test of `match` steps, we offer extra pattern queries on modern graph.
  - [LDBC test](https://github.com/alibaba/GraphScope/blob/main/interactive_engine/compiler/src/main/java/com/alibaba/graphscope/gremlin/integration/suite/ldbc): We further test GIE against the LDBC complex workloads on the LDBC social network with the scale factor (sf) 1.
   Please refer to the [tutorial](./tutorial_ldbc_gremlin.md) for more information.

## Build and Test GIE with Vineyard Store
In [GIE standalone deployment](./deployment.md), we have instructed on how to deploy GIE in a
Kubenetes cluster with Vineyard store. Here, we show how to develop and test GIE with vineyard
store on a local machine.

You could build the GIE engine (on vineyard store) with the following command:
```bash
./gs make interactive --storage-type=vineyard
```

You could locally test the GIE engine (on vineyard store) with a single command:
```bash
./gs test interactive --local --storage-type=vineyard
```

If you want to individually start the executor role, you can follow the steps below:

1. First, make sure that a Vineyard service is already running and a graph has been successfully loaded. Let's assume the object ID of the graph is 7541917260097168.

2. Set the GRAPHSCOPE_HOME environment variable:
```bash
export GRAPHSCOPE_HOME=<your_local_repo>/interactive_engine/assembly/target/graphscope
```
3. Configure the `$GRAPHSCOPE_HOME/conf/executor.vineyard.properties` file:
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
graph.vineyard.object.id: 7541917260097168
```

4. Start the `gaia_executor`:
```bash
$GRAPHSCOPE_HOME/bin/gaia_executor $GRAPHSCOPE_HOME/conf/log4rs.yml $GRAPHSCOPE_HOME/conf/executor.vineyard.properties &
```

If you want to individually start the frontend role, follow these steps:

1. Configure the `$GRAPHSCOPE_HOME/conf/frontend.vineyard.properties` file:
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
graph.schema = /tmp/7541917260097168.json

## Frontend Config
frontend.service.port = 8182

# disable authentication if username or password is not set
# auth.username = default
# auth.password = default
```

2. Start the `frontend`:
```bash
java -cp ".:$GRAPHSCOPE_HOME/lib/*" -Djna.library.path=$GRAPHSCOPE_HOME/lib com.alibaba.graphscope.frontend.Frontend $GRAPHSCOPE_HOME/conf/frontend.vineyard.properties &
```

To access the frontend service, visit localhost:8182 in your gremlin console.
