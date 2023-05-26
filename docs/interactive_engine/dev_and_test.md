# Dev and Test

This document describes how to build and test GraphScope Interactive Engine from source code.

## Dev Environment

Here we would use a prebuilt docker image with necessary dependencies installed.

```bash
docker run --name dev -it --shm-size=4096m registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
```

Please refer to [Dev Environment](../development/dev_guide.md#dev-environment) to find more options to get a dev environment.

## Test GIE with Local Experimental Store
 You first set the working directory to local repo. If you are in the above
 dev container, it should be `/workspaces/GraphScope`.
```bash
export GRAPHSCOPE_HOME=`pwd`
# Here the `pwd` is the root path of GraphScope repository
```
See more about `GRAPHSCOPE_HOME` in [run tests](../development/how_to_test.md#run-tests)


You could locally test the GIE engine with a single command:
```bash
./gs test interactive --local
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

