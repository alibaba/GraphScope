# Dev and Test

This document describe how the source code of Interactive is organized, and how to build `Interactive` from source and run tests.

## Dev Environment

Before building `Interactive` from the source code, you need to set up a development environment with numerous dependencies.
Here, we offer two options: installing all dependencies on the local machine or building it within the provided Docker image

### Install Deps on Local

To install all dependencies on your local machine, run the following code with command-line utility script `gsctl.py`.

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

The code for Interactive Query engine is under folder `flex`, and is organized as follows.
- `codegen`: A binary `gen_code_from_plan` is built from this repo, which is able to generate c++ code from a physical plan.
- `engines`: 
    - `engines/graph_db`: Provide the interface and implementation of `GraphDB`, which represents the storage of the graph.
    - `engines/hqps_db`: Contains the implementation Graph Query Engine, including the data structures and implementation of physical operators.
    - `engines/http_server`: Contains the http server based on seastar httpd, and the definition of actors of [hiactor](https://github.com/alibaba/hiactor).
- `interactive`: Contains the product related stuffs.
    - `interactive/docker`: The dockerfile of interactive.
    - `interactive/examples`: Some example graph definition and raw data.
    - `interactive/openapi/openapi_interactive`: The openapi specification for interactive RESTful API.
- `storages`:
    - `storages/metadata`: The implementation of metadata store.
    - `storages/immutable_graph`: The implementation of immutable graph storage.
    - `storages/rt_mutable_graph`: The implementation of a mutable graph storage, which is based on `mutable_csr`.
- `tests`: Contains the test cases and scripsts
- `third_party`: Third party dependencies.
- `utils`: Utility class and functions.


### Compiler

The Compiler plays an important role in Interactive by translating graph queries expressed in graph query languages (Cypher/Gremlin) into physical query plans based on GAIA IR.
The code for Compiler is located under `interactive_engine/compiler`.
For more detail information about compiler, please check [this documentation](../../../interactive_engine/design_of_gie.md)

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

There are many test cases designed for Interactive, you can refer to [interactive.yaml](https://github.com/alibaba/GraphScope/blob/main/.github/workflows/interactive.yml) for the github workflow.
Here is a simple test case for verifying the correctness of SDK and interactive admin service.

First we need to create a directory as Interactive workspace, and try to create a new graph with name `modern_graph` and import data to the graph.

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

Workspace is like the data directory for a database, where the metadata and graph data is placed. Here is an example.
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



There is an `engine_config_test.yaml` file in the `${GITHUB_WORKSPACE}/flex/tests/hqps` directory, 
change the `default_graph` field to `modern_graph` as we intend to test on `modern_graph`.

```bash
cd ${GITHUB_WORKSPACE}/flex/tests/hqps
sed -i 's/default_graph: ldbc/default_graph: modern_graph/g' ./engine_config_test.yaml
```

Then we run a script `hqps_admin_test.sh` to verify the correctness of interactive admin service.

```bash
cd ${GITHUB_WORKSPACE}/flex/tests/hqps
# Change the default_graph field to 
bash hqps_admin_test.sh ${TMP_INTERACTIVE_WORKSPACE} ./engine_config_test.yaml ${GS_TEST_DIR}
```