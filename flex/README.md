# GraphScope Flex: A Graph Computing Stack with LEGO-Like Modularity

### Introduction

Graph applications in real life have diverse workloads, programming interfaces, and storage formats. GraphScope is a one-stop solution that addresses these variations. However, challenges remain due to:
- Various deployment modes are needed, such as an offline analytical pipeline for low latency, online services requiring high throughput, or a learning task benefiting from heterogeneous hardware.
- An all-inclusive solution may not be the best option as it could lead to increased resource and cost requirements.

GraphScope Flex is an ongoing evolution of GraphScope. It champions a modular design that diminishes resource and cost requirements while delivering a seamless, user-friendly interface for flexible deployment. Presently, GraphScope Flex is actively being developed.

### Architecture

<div align="center">
    <img src="/docs/images/gsflex-overview.png" width="600" alt="GraphScope Flex architecture" />
</div>

The GraphScope Flex stack (as shown in the figure), consists of multiple components that users can combine like LEGO bricks to customize their graph computing deployments. The components are classified into three layers: 
- Application Layer, which includes pre-built libraries of algorithms and GNN models, as well as SDKs and APIs; 
- Execution Layer, which comprises multiple engines that are specialized for their respective domains; 
- Storage Layer, which establishes a uniform interface for managing graph data across various storage backends. 

## How to Build

### Dependencies

Please use `script/install_deps.sh` to install dependencies.
Alternatively, you can manually install a subset of dependencies required by your selected components.

Please refer to [scripts/install_deps.sh](https://github.com/alibaba/GraphScope/blob/main/flex/scripts/install_dependencies.sh) for the full list of dependencies.

### Building 

GraphScope Flex comes with a useful script `flexbuild` that allows you to build a customized stack using specific components. `flexbuild` has  some parameters and two of them are critical for building:

- argument `COMPONENTS` specifies which "lego bricks" you want to select. The available components are illustrated in figure above or listed in the `--help` section.
- flag `--app` specifies the application type of the built artifacts you want to build. The available types are `db`, `olap`, `ldbcdriver`, `docker`(WIP).

By selecting and combining the components that best suit your requirements, you can use the `flexbuild` script to create a tailored deployment of GraphScope Flex for your specific use case.

Please use `flexbuild --help` to learn more.

## User Cases and Examples

### Case 1: For online BI analysis

<div align="center">
    <img src="/docs/images/gsflex-case1.png" width="500" alt="GraphScope Flex usecase-1" />
</div>

BI analysis is for analysts who interactively analyze data in a WebUI. While high concurrency is unlikely, low latency for complex queries is crucial.

GraphScope Flex compiles Cypher and Gremlin queries into a **unified intermediate representation (IR)** and optimizes it using a **universal query optimizer** and **catalog** module. The optimized IR is passed to **Gaia Codegen** and executed on **Gaia**, a distributed dataflow engine that reduces query latency through data parallelism. Graph data is accessed from a mutable csr-based persistent storage via a unified interface.

To build the artifacts for this use case, run the following command:
```bash
./flexbuild cypher gaia cppsp mcsr --app db
# To be supported. Please try scripts for other cases.
```

### Case 2: For high QPS queries

<div align="center">
    <img src="/docs/images/gsflex-case2.png" width="500" alt="GraphScope Flex usecase-2" />
</div>

In some service scenarios, e.g., recommendation or searching, the graph queries are coming at an extremely high rate and demands high throughput. In these scenarios, GraphScope Flex can be deployed with a different component set. The **compiler** generates an optimized query plan and **Hiactor Codegen** produces a physical plan tailored for **Hiactor**, a high-performance and concurrent actor framework for OLTP-like queries.

To build the artifacts for this use case, run the following command:
```bash
./flexbuild hiactor cppsp mcsr --app db
```

Please note that we use the artifacts built by this command for LDBC SNB benchmarking.


### Case 3: For offline graph analytics

<div align="center">
    <img src="/docs/images/gsflex-case3.png" width="500" alt="GraphScope Flex usecase-3" />
</div>

GraphScope Flex is an efficient and user-friendly platform for performing graph analytics. It offers **built-in algorithms**, as well as **interfaces** for developing customized algorithms. The runtime, based on **GRAPE**, is fragment-centric and extensible, supporting multiple programming models like **FLASH**, **PIE**, and **Pregel**. Sequential algorithms can be easily parallelized or incrementalized using the **Ingress** component. To achieve high performance, an **in-memory graph store** is deployed in this stack.

To build the artifacts for this use case, run the following command:
```bash
./flexbuild builtin grape-cpu --app olap
# or
./flexbuild builtin grape-gpu --app ldbcdriver
```

### Case 4: For graph learning tasks

<div align="center">
    <img src="/docs/images/gsflex-case4.png" width="500" alt="GraphScope Flex usecase-4" />
</div>

GraphScope Flex's GNN framework supports billion-scale graphs in industrial scenarios. It provides GNN model development paradigms, **example models**,  and the flexibility to choose between **TensorFlow** or **PyTorch** as the training backend. Furthermore, the framework employs decoupled sampling and training processes, which can be independently scaled for optimal end-to-end throughput, providing superior performance.

To build the artifacts for this use case, run the following command:
```bash
./flexbuild gnnmodels graphlearn tensorflow vineyard --app gnn
```
