# Dev and Test

This document describes the organization of the source code for Interactive and provides instructions on how to build `Interactive` from source and run tests.

## Dev Environment

Before building `Interactive` from the source code, you need to set up a development environment with various dependencies. 
Here, we provide two options: installing all dependencies on the local machine or building it within the provided Docker image.

### Install Deps on Local

To install all dependencies on your local machine, run the following code with the command-line utility script `gsctl.py`.

```bash
python3 gsctl.py install-deps dev
```

> Currently, Interactive cannot be built from source on macOS. However, you can attempt to build Interactive on the Docker image, as our Docker image supports the 'arm64' platform.

### Develop on Docker Container

We provided a docker image `graphscope-dev` with all tools and dependencies included.

```bash
docker run -it registry.cn-hongkong.aliyuncs.com/graphscope/graphscope-dev:latest
```

Or you can open the codebase of GraphScope in [dev container](../../../development/dev_guide.md#develop-with-dev-containers).

## Understanding the Codebase

Interactive is composed of two parts, the execution engine and frontend compiler. 

### Interactive Query Engine

The Interactive Query Engine code is organized in the `flex` folder as follows:
- `codegen`: The binary `gen_code_from_plan` is built from this repository, capable of generating C++ code from a physical plan.
- `engines`:
    - `engines/graph_db`: Provides the interface and implementation of `GraphDB`, which manages graph storage.
    - `engines/hqps_db`: Includes the Graph Query Engine implementation, data structures, and physical operators.
    - `engines/http_server`: Incorporates the HTTP server based on Seastar httpd and defines actors in [hiactor](https://github.com/alibaba/hiactor).
- `interactive`: Contains product-related components.
    - `interactive/docker`: Dockerfile for Interactive.
    - `interactive/examples`: Graph definition examples and raw data.
    - `interactive/openapi/openapi_interactive`: OpenAPI specification for Interactive's RESTful API.
- `storages`:
    - `storages/metadata`: Implementation of the metadata store.
    - `storages/immutable_graph`: Implementation of immutable graph storage.
    - `storages/rt_mutable_graph`: Implementation of mutable graph storage based on `mutable_csr`.
- `tests`: Includes test cases and scripts.
- `third_party`: Contains third-party dependencies.
- `utils`: Utility classes and functions.



### Compiler

The Compiler is crucial in Interactive as it converts graph queries written in graph query languages (Cypher/Gremlin) into physical query plans using GAIA IR.
The Compiler code is found in `interactive_engine/compiler`.
For additional details on the Compiler, refer to [this documentation](../../../interactive_engine/design_of_gie.md)

## Build Interactive

First, build the Interactive Query Engine.

```bash
git clone https://github.com/alibaba/GraphScope # or replace with your own forked repo
cd GraphScope 
git submodule update --init
cd flex
mkdir build && cd build
cmake ..
make -j
```

Then, build the Compiler.
```bash
cd interactive_engine
mvn clean package -DskipTests -Pexperimental
```

## Testing

Numerous test cases have been created for Interactive, which can be referenced in the GitHub workflow[interactive.yaml](https://github.com/alibaba/GraphScope/blob/main/.github/workflows/interactive.yml).
Below is a basic test case for validating the accuracy of the SDK and interactive admin service.

Initially, a directory needs to be established as the Interactive workspace, then proceed to create a new graph named `modern_graph` and import data into the graph.

```bash
# Clone the testing data
export GS_TEST_DIR=/tmp/gstest
git clone -b master --single-branch --depth=1 https://github.com/GraphScope/gstest.git ${GS_TEST_DIR}

export GITHUB_WORKSPACE=/path/to/GraphScope
export FLEX_DATA_DIR=${GITHUB_WORKSPACE}/flex/interactive/examples/modern_graph
export TMP_INTERACTIVE_WORKSPACE=/tmp/temp_workspace
cd ${GITHUB_WORKSPACE}/flex/build/

# Create interactive workspace
mkdir -p ${TMP_INTERACTIVE_WORKSPACE}
SCHEMA_FILE=${GITHUB_WORKSPACE}/flex/interactive/examples/modern_graph/graph.yaml
BULK_LOAD_FILE=${GITHUB_WORKSPACE}/flex/interactive/examples/modern_graph/bulk_load.yaml

# Create a directory to put modern_graph's schema.yaml and graph data
mkdir -p ${TMP_INTERACTIVE_WORKSPACE}/data/modern_graph/
cp ${SCHEMA_FILE} ${TMP_INTERACTIVE_WORKSPACE}/data/modern_graph/graph.yaml

# Load data to modern_graph
GLOG_v=10 ./bin/bulk_loader -g ${SCHEMA_FILE} -l ${BULK_LOAD_FILE} -d ${TMP_INTERACTIVE_WORKSPACE}/data/modern_graph/indices/
```

A workspace is akin to the data directory for a database, housing metadata and graph data. Here is an example.

```txt
/tmp/temp_workspace
├── data
│   ├── 1 -> /tmp/temp_workspace/data/modern_graph
│   ├── 2
│   └── modern_graph
└── METADATA
    ├── GRAPH_META
    ├── INDICES_LOCK
    ├── JOB_META
    ├── PLUGIN_META
    ├── PLUGINS_LOCK
    └── RUNNING_GRAPH
```



In the `${GITHUB_WORKSPACE}/flex/tests/hqps` directory, there is an `engine_config_test.yaml` file.
Modify the `default_graph` field to `modern_graph` for testing purposes on `modern_graph`.

```bash
cd ${GITHUB_WORKSPACE}/flex/tests/hqps
sed -i 's/default_graph: ldbc/default_graph: modern_graph/g' ./engine_config_test.yaml
```

Subsequently, execute the `hqps_admin_test.sh` script to test the of the interactive admin service.

```bash
cd ${GITHUB_WORKSPACE}/flex/tests/hqps
# Change the default_graph field to 
bash hqps_admin_test.sh ${TMP_INTERACTIVE_WORKSPACE} ./engine_config_test.yaml ${GS_TEST_DIR}
```