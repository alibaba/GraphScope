# Graph-Analytics-Benchmarks

**Graph-Analytics-Benchmarks** accompanies the paper [*Revisiting Graph Analytics Benchmark*](https://doi.org/10.1145/3725345), published in *Proceedings of the ACM on Management of Data (SIGMOD 2025)*.
This project introduces a new benchmark that overcomes limitations of existing suites and enables apples-to-apples comparisons across graph platforms.

---



## Table of Contents

1. [Overview](#overview)  
2. [Quick Start](#quick-start)  
   - [Data Generator](#data-generator)  
   - [LLM-based Usability Evaluation](#llm-based-usability-evaluation)  
     - [Environment Setup](#environment-setup)  
     - [Running the Program](#running-the-program)  
   - [Performance Evaluation](#performance-evaluation)  
     - [Platform Groups](#platform-groups)  
     - [Platforms and Configurations](#platforms-and-configurations)  
       - [Flash](#flash)  
       - [Ligra](#ligra)  
       - [Grape](#grape)  
       - [Pregel+](#pregel)  
       - [Gthinker](#gthinker)  
       - [PowerGraph](#powergraph)  
       - [GraphX](#graphx)  
3. [Cite This Work](#cite-this-work)  


## Overview

**Graph-Analytics-Benchmarks** accompanies the paper [“Revisiting Graph Analytics Benchmark”](https://doi.org/10.1145/3725345), which introduces a new benchmark suite for cross-platform graph analytics.  
The paper argues that existing suites (e.g., LDBC Graphalytics) fall short in fully capturing differences across platforms, and proposes a benchmark enabling fair, scalable, and reproducible comparisons.

**This repository provides three main components:**
- **Failure-Free Trial Data Generator (FFT-DG)**:  
  A lightweight, failure-immune data generator with independent control over **scale**, **density**, and **diameter**. Supports multiple output formats (including weighted/unweighted edge lists).
- **LLM-based API Usability Evaluation**:  
  A multi-level LLM framework for automatically generating and evaluating algorithm implementations across platforms, producing multi-dimensional quality scores and replacing costly human studies (packaged in Docker for one-command execution).
- **Performance Evaluation Scripts**:  
  Reproducible experiment setup in **Kubernetes + Docker**, with distributed jobs scheduled by the **Kubeflow MPI Operator (MPIJob)**. Provides unified reporting on **timing, throughput (edges/s), scalability, and robustness**.

**Algorithm Coverage (8 representative algorithms):**  
PageRank (PR), Single-Source Shortest Path (SSSP), Triangle Counting (TC), Betweenness Centrality (BC), K-Core (KC), Community Detection (CD), Label Propagation (LPA), Weakly Connected Components (WCC).

**Supported Platforms and Execution Modes:**
- **Kubernetes + MPI**: Flash, Ligra, Grape  
- **Kubernetes + MPI + Hadoop**: Pregel+, Gthinker, PowerGraph  
- **Spark-based**: GraphX (requires Spark 2.4.x / Scala 2.11 / Hadoop 2.7 / Java 8)

**Intended Audience:**  
Researchers, practitioners, and educators in graph systems and distributed computing who require reproducible, apples-to-apples comparisons and system tuning under consistent conditions.

> For citation details, see [Cite This Work](#cite-this-work).


## Quick Start

### Data Generator

We provide a lightweight C++ program (download from [Data_Generator.zip](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/Data_Generator.zip)) to generate data. It takes three parameters:

  - **Scale**: the dataset scale, chosen from `8, 9, 10`. You may also set a custom size.
  - **Platform**: the target platform to control the output format. Custom formats are supported.
  - **Feature**: the dataset feature (*Standard*; *Density* with **higher** edge density; *Diameter* with a **larger** diameter).


```shell
unzip Data_Generator.zip
cd Data_Generator
g++ FFT-DG.cpp -o generator -O3
./generator 8 graphx Standard
```

We also provide a [LDBC-version of our generator](https://github.com/Lingkai981/Graph-Analytics-Benchmarks/tree/e2377e5a5a1e752ed3db44c58b8c95afc80ae030/renewal_datagen) consisting of only a few modifications.

To ease startup, here are the datasets used in our evaluation. The format of these datasets is an edge list, i.e., each line is a single edge.

[S8-Std](
https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-8-Standard.txt), 
[S8-Density](
https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-8-Density.txt),
[S8-Diameter](
https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-8-Diameter.txt),
[S9-Std](
https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-9-Standard.txt), 
[S9-Density](
https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-9-Density.txt),
[S9-Diameter](
https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-9-Diameter.txt),
[S10-Std](
https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-10-Standard.txt),


### LLM-based Usability Evaluation

> This framework includes an automated code generator and a code evaluator powered by LLMs. It supports multiple graph platforms and common algorithm implementations, producing platform-compliant code and multi-dimensional quality scores.


#### Environment Setup
Download Docker image file [llm-eval.tar](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/llm-eval.tar)
```shell
docker load -i llm-eval.tar
```
#### Running the Program
```shell
docker run --rm \
  -e OPENAI_API_KEY=<your OPENAI_API_KEY> \
  -e PLATFORM=<your platform> \
  -e ALGORITHM=<your algorithm> \
  llm-eval
```
or

```shell
docker run -it --rm -e OPENAI_API_KEY=<your OPENAI_API_KEY> llm-eval
```

### Performance Evaluation

> All performance experiments are conducted in a **Kubernetes cluster** with **Docker containers** to ensure reproducibility and consistency.  
> Distributed jobs are scheduled with the **Kubeflow MPI Operator (MPIJob)**.

#### Platform Groups

- **Kubernetes + MPI Operator**
  - Flash
  - Ligra
  - Grape
- **Kubernetes + MPI Operator + Hadoop**
  - Pregel+
  - Gthinker
  - PowerGraph
- **Spark-based Environment**
  - GraphX (requires Spark 2.4.x, Scala 2.11, Hadoop 2.7, Java 8)

---

#### Platforms and Configurations

##### Flash

- **Dataset Format**: The dataset is organized in folders named according to the following patterns:
  - For the **sssp** algorithm: `flash-sssp-edges-{SCALE}-{FEATURE}` (e.g., `flash-sssp-edges-8-Standard`)
  - For other algorithms: `flash-edges-{SCALE}-{FEATURE}` (e.g., `flash-edges-8-Standard`)
  
  Each folder contains the following files:
  - `graph.txt`: The graph data in text format.
  - `graph.idx`: The index file for the graph data.
  - `graph.dat`: The data file for the graph.

- **Supported Algorithms**: 
  - `pagerank`, `sssp`, `triangle`, `lpa`, `k-core-search`, `clique`, `cc`, `bc`

- **Run Flash**:  
   Follow these steps to run the algorithm:

   1. Download and load the Docker image [flash-mpi-v0.4.tar](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-mpi-v0.4.tar) on all machines.
     ```bash
     sudo docker load -i flash-mpi-v0.4.tar
     ```
   2. On all machines, create identical folders to store datasets. Then, download and unzip the following datasets and place them into these folders:
      - [flash-edges-8-Standard](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-edges-8-Standard.zip)
      - [flash-edges-9-Standard](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-edges-9-Standard.zip)
      - [flash-edges-8-Density](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-edges-8-Density.zip)
      - [flash-edges-9-Density](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-edges-9-Density.zip)
      - [flash-edges-8-Diameter](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-edges-8-Diameter.zip)
      - [flash-edges-9-Diameter](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-edges-9-Diameter.zip)
      - [flash-sssp-edges-8-Standard](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-sssp-edges-8-Standard.zip)
      - [flash-sssp-edges-9-Standard](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-sssp-edges-9-Standard.zip)
      - [flash-sssp-edges-8-Density](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-sssp-edges-8-Density.zip)
      - [flash-sssp-edges-9-Density](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-sssp-edges-9-Density.zip)
      - [flash-sssp-edges-8-Diameter](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-sssp-edges-8-Diameter.zip)
      - [flash-sssp-edges-9-Diameter](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-sssp-edges-9-Diameter.zip)

   3. Execute the following command to run the desired algorithm:

     ```bash
     cd Flash
     ./run.sh <ALGORITHM> <PATH_TO_DATASET_FOLDER>
     ```

     - `<ALGORITHM>`: Replace with the name of the algorithm you want to run (e.g., `sssp`, `pagerank`, etc.).
     - `<PATH_TO_DATASET_FOLDER>`: Provide the path to the folder where the dataset is stored.
     - The output logs will be generated in the `Flash/output/` folder, with the following naming format:  
       ```
       ${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log
       ```



##### Ligra

- **Dataset Format**:  
  The dataset for Ligra is provided as the `.txt` format.
    - For the **sssp** algorithm: `ligra-sssp-adj-{SCALE}-{FEATURE}.txt` (e.g., `ligra-sssp-adj-8-Diameter.txt`)
    - For other algorithms: `ligra-adj-{SCALE}-{FEATURE}` (e.g., `ligra-adj-8-Diameter.txt`)

- **Supported Algorithms**:  
  - `PageRank`, `BellmanFord`, `BC`, `KCLIQUE`, `KCore`, `LPA`, `Components`, `Triangle`

- **Run Ligra**:  
   Follow these steps to run the algorithm:

   1. Download and load the Docker image [ligra-mpi-v0.1.tar](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-mpi-v0.1.tar) on all machines.
    ```bash
     sudo docker load -i ligra-mpi-v0.1.tar
     ```
   2. On all machines, create identical folders to store datasets. Then, download the following datasets and place them into these folders:
      - [ligra-adj-8-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-adj-8-Standard.txt)
      - [ligra-adj-9-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-adj-9-Standard.txt)
      - [ligra-adj-8-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-adj-8-Density.txt)
      - [ligra-adj-9-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-adj-9-Density.txt)
      - [ligra-adj-8-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-adj-8-Diameter.txt)
      - [ligra-adj-9-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-adj-9-Diameter.txt)
      - [ligra-sssp-adj-8-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-sssp-adj-8-Standard.txt)
      - [ligra-sssp-adj-9-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-sssp-adj-9-Standard.txt)
      - [ligra-sssp-adj-8-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-sssp-adj-8-Density.txt)
      - [ligra-sssp-adj-9-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-sssp-adj-9-Density.txt)
      - [ligra-sssp-adj-8-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-sssp-adj-8-Diameter.txt)
      - [ligra-sssp-adj-9-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-sssp-adj-9-Diameter.txt)
        
   3. Execute the following command to run the desired algorithm:

      ```bash
      cd Ligra
      ./run.sh <ALGORITHM> <PATH_TO_DATASET_FOLDER>
      ```

      - `<ALGORITHM>`: Replace with the name of the algorithm you want to run (e.g., `BellmanFord`, `PageRank`, etc.).
      - `<PATH_TO_DATASET_FOLDER>`: Provide the path to the dataset folder.
      - The output logs will be generated in the `Ligra/output/` folder, with the following naming format:  
       ```
       ${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log
       ```
##### Grape

- **Dataset Format**:  
  Grape uses a simple vertex/edge list format, typically stored in two separate files.
    - Vertex File: A file with a .v extension, where each line represents a vertex ID.
      - Format: for the **sssp** algorithm: `grape-sssp-edges-{SCALE}-{FEATURE}.v` (e.g., `grape-sssp-edges-8-Standard.v`); and for other algorithm: `grape-edges-{SCALE}-{FEATURE}.v` (e.g., `grape-edges-8-Standard.v`).
    - Edge File: A file with a .e extension, where each line represents a directed edge (and optionally, a weight).
      - Format: for the **sssp** algorithm: `grape-sssp-edges-{SCALE}-{FEATURE}.e` (e.g., `grape-sssp-edges-8-Standard.e`); and for other algorithm: `grape-edges-{SCALE}-{FEATURE}.e` (e.g., `grape-edges-8-Standard.e`).
      
- **Supported Algorithms**:  
  - `pagerank`, `sssp`, `bc`, `kclique`, `core_decomposition`, `cdlp`, `wcc`, `lcc`

- **Run Grape**:  
   Follow these steps to run the algorithm:

   1. Download and load the Docker image [grape-mpi-v0.1.tar](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-mpi-v0.1.tar) on all machines.
    ```bash
     sudo docker load -i grape-mpi-v0.1.tar
     ```
   2. On all machines, create identical folders to store datasets. Then, download the following datasets and place them into these folders:
      - [grape-edges-8-Standard.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-8-Standard.v)
      - [grape-edges-9-Standard.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-9-Standard.v)
      - [grape-edges-8-Density.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-8-Density.v)
      - [grape-edges-9-Density.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-9-Density.v)
      - [grape-edges-8-Diameter.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-8-Diameter.v)
      - [grape-edges-9-Diameter.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-9-Diameter.v)
      - [grape-sssp-edges-8-Standard.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-8-Standard.v)
      - [grape-sssp-edges-9-Standard.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-9-Standard.v)
      - [grape-sssp-edges-8-Density.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-8-Density.v)
      - [grape-sssp-edges-9-Density.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-9-Density.v)
      - [grape-sssp-edges-8-Diameter.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-8-Diameter.v)
      - [grape-sssp-edges-9-Diameter.v](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-9-Diameter.v)
      - [grape-edges-8-Standard.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-8-Standard.e)
      - [grape-edges-9-Standard.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-9-Standard.e)
      - [grape-edges-8-Density.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-8-Density.e)
      - [grape-edges-9-Density.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-9-Density.e)
      - [grape-edges-8-Diameter.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-8-Diameter.e)
      - [grape-edges-9-Diameter.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-edges-9-Diameter.e)
      - [grape-sssp-edges-8-Standard.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-8-Standard.e)
      - [grape-sssp-edges-9-Standard.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-9-Standard.e)
      - [grape-sssp-edges-8-Density.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-8-Density.e)
      - [grape-sssp-edges-9-Density.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-9-Density.e)
      - [grape-sssp-edges-8-Diameter.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-8-Diameter.e)
      - [grape-sssp-edges-9-Diameter.e](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-sssp-edges-9-Diameter.e)
        
   3. Execute the following command to run the desired algorithm:

      ```bash
      cd Grape
      ./run.sh <ALGORITHM> <PATH_TO_DATASET_FOLDER>
      ```

      - `<ALGORITHM>`: Replace with the name of the algorithm you want to run (e.g., `sssp`, `pagerank`, etc.).
      - `<PATH_TO_DATASET_FOLDER>`: Provide the path to the dataset folder.
      - The output logs will be generated in the `Grape/output/` folder, with the following naming format:  
       ```
       ${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log
       ```
##### Pregel+

- **Dataset Format**:  
  The dataset for Pregel+ is provided as the `.txt` format.
    - Format: `pregel+-adj-{SCALE}-{FEATURE}.txt` (e.g., `pregel+-adj-8-Standard.txt`)
    
- **Supported Algorithms**:  
  - `pagerank`, `sssp`, `betweenness`, `lpa`, `clique`, `triangle`, `cc`

- **Run Pregel+**:  
   Follow these steps to run the algorithm:

   1. Download and load the Docker image [pregel-mpi-v0.1.tar](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel-mpi-v0.1.tar) on all machines.
    ```bash
     sudo docker load -i pregel-mpi-v0.1.tar
     ```
   2. On all machines, create identical folders to store datasets. Then, download the following datasets and place them into these folders:
      - [pregel+-adj-8-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-8-Standard.txt)
      - [pregel+-adj-9-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-9-Standard.txt)
      - [pregel+-adj-8-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-8-Density.txt)
      - [pregel+-adj-9-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-9-Density.txt)
      - [pregel+-adj-8-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-8-Diameter.txt)
      - [pregel+-adj-9-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-9-Diameter.txt)
        
   3. Execute the following command to run the desired algorithm:
      - **Important**: In file `pregel-mpijob-template.yaml`, `my-hadoop-cluster-hadoop` **must match** the name of the `ConfigMap` you create or reference in your Kubernetes YAML configurations. If the `ConfigMap` is named differently, you will need to update the name in the `volumes` section of the pod definition.

      ```bash
      cd Pregel+
      ./run.sh <ALGORITHM> <PATH_TO_DATASET_FOLDER>
      ```

      - `<ALGORITHM>`: Replace with the name of the algorithm you want to run (e.g., `sssp`, `pagerank`, etc.).
      - `<PATH_TO_DATASET_FOLDER>`: Provide the path to the dataset folder.
      - The output logs will be generated in the `Pregel+/output/` folder, with the following naming format:  
       ```
       ${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log
       ```
##### Gthinker

- **Dataset Format**:  
  The dataset for Gthinker is provided as the `.txt` format.
    - Format: `gthinker-adj-{SCALE}-{FEATURE}.txt` (e.g., `gthinker-adj-8-Standard.txt`)
    
- **Supported Algorithms**:  
  - `clique`, `triangle`

- **Run Gthinker**:  
   Follow these steps to run the algorithm:

   1. Download and load the Docker image [gthinker-mpi-v0.1.tar](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/gthinker-mpi-v0.1.tar) on all machines.
    ```bash
     sudo docker load -i gthinker-mpi-v0.1.tar
     ```
   2. On all machines, create identical folders to store datasets. Then, download datasets (**same to datasets of Pregel+**) and place them into these folders.
      
        
   3. Execute the following command to run the desired algorithm:
      - **Important**: In file `gthinker-mpijob-template.yaml`, `my-hadoop-cluster-hadoop` **must match** the name of the `ConfigMap` you create or reference in your Kubernetes YAML configurations. If the `ConfigMap` is named differently, you will need to update the name in the `volumes` section of the pod definition.

      ```bash
      cd Gthinker
      ./run.sh <ALGORITHM> <PATH_TO_DATASET_FOLDER>
      ```

      - `<ALGORITHM>`: Replace with the name of the algorithm you want to run (e.g., `clique`, `triangle`).
      - `<PATH_TO_DATASET_FOLDER>`: Provide the path to the dataset folder.
      - The output logs will be generated in the `Gthinker/output/` folder, with the following naming format:  
       ```
       ${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log
       ```
##### PowerGraph

- **Dataset Format**:  
  The dataset for PowerGraph is provided as the `.txt` format.
    - Format: `graphlab-adj-{SCALE}-{FEATURE}.txt` (e.g., `graphlab-adj-8-Standard.txt`)
    
- **Supported Algorithms**:  
  - `pagerank`, `sssp`, `triangle`, `lpa`, `kcore`, `cc`, `betweenness`

- **Run PowerGraph**:  
   Follow these steps to run the algorithm:

   1. Download and load the Docker image [graphlab-mpi-v0.1.tar](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-mpi-v0.1.tar) on all machines.
    ```bash
     sudo docker load -i graphlab-mpi-v0.1.tar
     ```
   2. On all machines, create identical folders to store datasets. Then, download the following datasets and place them into these folders:
      - [graphlab-adj-8-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-8-Standard.txt)
      - [graphlab-adj-9-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-9-Standard.txt)
      - [graphlab-adj-8-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-8-Density.txt)
      - [graphlab-adj-9-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-9-Density.txt)
      - [graphlab-adj-8-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-8-Diameter.txt)
      - [graphlab-adj-9-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-9-Diameter.txt)
        
   3. Execute the following command to run the desired algorithm:
      - **Important**: In file `graphlab-mpijob-template.yaml`, `my-hadoop-cluster-hadoop` **must match** the name of the `ConfigMap` you create or reference in your Kubernetes YAML configurations. If the `ConfigMap` is named differently, you will need to update the name in the `volumes` section of the pod definition.

      ```bash
      cd PowerGraph
      ./run.sh <ALGORITHM> <PATH_TO_DATASET_FOLDER>
      ```

      - `<ALGORITHM>`: Replace with the name of the algorithm you want to run (e.g., `clique`, `triangle`).
      - `<PATH_TO_DATASET_FOLDER>`: Provide the path to the dataset folder.
      - The output logs will be generated in the `PowerGraph/output/` folder, with the following naming format:  
       ```
       ${ALGORITHM}-${DATASET_NAME}-n${machines}-p${SLOTS_PER_WORKER}.log
       ```
##### GraphX

- **Dataset Format**:  
  The dataset for Ligra is provided as the `.txt` format.
    - Format: `graphx-adj-{SCALE}-{FEATURE}.txt` (e.g., `graphx-adj-8-Standard.txt`)

- **Supported Algorithms**:  
  - `pagerank`, `sssp`, `triangle`, `lpa`, `kcore`, `cc`, `betweenness`, `clique`

- **Run GraphX**:
  - **Environment Requirements**:
  The `.jar` files are compiled with **Scala 2.11** (`_2.11` suffix). To ensure compatibility, use the following environment:

  - **Java**: OpenJDK/Oracle JDK **8** (1.8, recommended `1.8.0_202` or later)
  - **Scala**: **2.11.12** (only required if you plan to recompile or use Scala REPL)
  - **Apache Spark**: **2.4.8** (*Pre-built for Hadoop 2.7*)
    - Spark 2.4.x is the last major version compiled with Scala 2.11.  
    - **Do not use Spark 3.x** (requires Scala 2.12+).
  - **Hadoop**: **2.7.x**
  
   Follow these steps to run the algorithm:
  
   1. Download the following datasets and place them into these folders:
      - [graphx-edges-8-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-8-Standard.txt)
      - [graphx-edges-9-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-9-Standard.txt)
      - [graphx-edges-8-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-8-Density.txt)
      - [graphx-edges-9-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-9-Density.txt)
      - [graphx-edges-8-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-8-Diameter.txt)
      - [graphx-edges-9-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-edges-9-Diameter.txt)
      - [graphx-weight-edges-8-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-weight-edges-8-Standard.txt)
      - [graphx-weight-edges-9-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-weight-edges-9-Standard.txt)
      - [graphx-weight-edges-8-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-weight-edges-8-Density.txt)
      - [graphx-weight-edges-9-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-weight-edges-9-Density.txt)
      - [graphx-weight-edges-8-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-weight-edges-8-Diameter.txt)
      - [graphx-weight-edges-9-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphx-weight-edges-9-Diameter.txt)
        
   2. Download the `.jar` files and command files into the same folder and execute the command to run the algorithm:
      - `pagerank`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pagerankexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pagerank.sh))
      - `sssp`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ssspexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/sssp.sh))
      - `triangle`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/trianglecountingexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/trianglecounting.sh))
      - `lpa`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/labelpropagationexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/labelpropagation.sh))
      - `kcore`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/coreexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/core.sh))
      - `cc`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/connectedcomponentexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/connectedcomponent.sh))
      - `betweenness`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/betweennesscentralityexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/betweennesscentrality.sh))
      - `clique`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/kcliqueexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/kclique.sh))
 
        For example:
        
         ```bash
         ./pagerank.sh <SPARK_MASTER> <PATH_TO_DATASET_FOLDER>
         ```

         - `<SPARK_MASTER>`: Spark master URL, e.g., spark://my_spark:7077.
         - `<PATH_TO_DATASET_FOLDER>`: Provide the path to the dataset folder.



## Cite This Work

If you use this artifact, please cite the paper:

```bibtex
@article{meng2025revisiting,
  title={Revisiting Graph Analytics Benchmark},
  author={Meng, Lingkai and Shao, Yu and Yuan, Long and Lai, Longbin and Cheng, Peng and Li, Xue and Yu, Wenyuan and Zhang, Wenjie and Lin, Xuemin and Zhou, Jingren},
  journal={Proceedings of the ACM on Management of Data},
  volume={3},
  number={3},
  pages={1--28},
  year={2025},
  publisher={ACM New York, NY, USA}
}
```

---
