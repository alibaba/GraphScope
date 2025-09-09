# Graph-Analytics-Benchmarks

**Graph-Analytics-Benchmarks** accompanies the paper [*Revisiting Graph Analytics Benchmark*](https://doi.org/10.1145/3725345), published in *Proceedings of the ACM on Management of Data (SIGMOD 2025)*.
This project introduces a new benchmark that overcomes limitations of existing suites and enables apples-to-apples comparisons across graph platforms.

---



## Table of Contents

1. [Overview](#overview)  
2. [Quick Start](#quick-start)  
   - [Prerequisites](#prerequisites)  
   - [Installation & Setup](#installation--setup)  
   - [Commands](#commands)  
     - [Global help](#global-help)  
     - [`datagen` Data Generation](#datagen-data-generation)  
       - [Requirements](#requirements)  
       - [Usage](#usage)  
       - [Example](#example)  
     - [`llm-eval` LLM Usability Evaluation](#llm-eval-llm-usability-evaluation)  
       - [Requirements](#requirements-1)  
       - [Usage](#usage-1)  
       - [Example](#example-1)  
     - [`perf-eval` Performance Evaluation](#perf-eval-performance-evaluation)  
       - [Requirements](#requirements-2)  
       - [Usage](#usage-2)  
       - [Examples](#examples)  
3. [Datasets & Formats](#datasets--formats)  
   - [Flash](#flash)  
   - [Ligra](#ligra)  
   - [Grape](#grape)  
   - [Pregel+](#pregel)  
   - [Gthinker](#gthinker)  
   - [PowerGraph](#powergraph)  
   - [GraphX](#graphx)  
4. [Cite This Work](#cite-this-work)  





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


### Prerequisites

- **Python 3.8+** (recommended: 3.10/3.11)  
- **Docker** (required for `llm-eval` and platform images)  
- **Kubernetes + MPI Operator + Hadoop** (for distributed experiments)  
- (GraphX only) **Java 8 / Scala 2.11 / Spark 2.4.x (Hadoop 2.7)**

### Installation & Setup

```bash
# Configure environment variables (for llm-eval)
cp .env.example .env
# edit .env to set OPENAI_API_KEY=xxxx
```

> If using the offline Docker image (e.g., `llm-eval.tar`):  
> ```bash
> docker load -i llm-eval.tar
> ```


### Commands

#### Global help
```bash
python3 cli.py --help
```

#### `datagen` Data Generation

##### Requirements
We provide a lightweight C++ program (download from [Data_Generator.zip](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/Data_Generator.zip)) to generate data. Download and unzip to the `Data_Generator/` folder. We also provide a [LDBC-version of our generator](https://github.com/Lingkai981/Graph-Analytics-Benchmarks/tree/e2377e5a5a1e752ed3db44c58b8c95afc80ae030/renewal_datagen) consists of only a few modification.

##### Usage
```bash
python cli.py datagen --scale <8|9|10|custom>   --platform <flash|ligra|grape|gthinker|pregel+|powergraph|graphx>   --feature <Standard|Density|Diameter>
```

- Automatically compiles and runs `FFT-DG.cpp` in `Data_Generator/`.
- Produces platform-specific datasets.

##### Example
```bash
python cli.py datagen --scale 9 --platform flash --feature Standard
```

---

#### `llm-eval` LLM Usability Evaluation

##### Requirements
- `OPENAI_API_KEY` set in `.env` or system environment.
- Docker image `[llm-eval.tar](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/llm-eval.tar)` available. 

##### Usage
```bash
docker load -i llm-eval.tar
python cli.py llm-eval --platform <platform> --algorithm <algorithm>
```

##### Example
```bash
python cli.py llm-eval --platform flash --algorithm pagerank
```

---

#### `perf-eval` Performance Evaluation

##### Requirements
- All performance experiments are conducted in a properly configured **Kubernetes + MPI Operator (MPIJob) + Hadoop** environment to ensure reproducibility and consistency.  
- **GraphX** experiments additionally require a properly configured **Spark environment** (Spark 2.4.x, Scala 2.11, Hadoop 2.7, Java 8).
- Download and load the Docker image of different platforms on all machines:
   - [Flash](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/flash-mpi-v0.4.tar)
   - [Ligra](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ligra-mpi-v0.1.tar)
   - [Grape](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/grape-mpi-v0.1.tar)
   - [Pregel+](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel-mpi-v0.1.tar)
   - [Gthinker](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/gthinker-mpi-v0.1.tar)
   - [PowerGraph](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-mpi-v0.1.tar)
- On all machines, create identical folders to store datasets. Then, download [the datasets](#datasets--formats) we provide, which conform to the platform specifications (and unzip for Flash), and place them into identical folders on all machines.
- For **GraphX**, build a dedicated `GraphX/` folder, then place the following `.jar` files and command files into it before executing the command to run the algorithm:
   - `pagerank`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pagerankexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pagerank.sh))
   - `sssp`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/ssspexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/sssp.sh))
   - `triangle`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/trianglecountingexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/trianglecounting.sh))
   - `lpa`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/labelpropagationexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/labelpropagation.sh))
   - `kcore`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/coreexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/core.sh))
   - `cc`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/connectedcomponentexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/connectedcomponent.sh))
   - `betweenness`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/betweennesscentralityexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/betweennesscentrality.sh))
   - `clique`([[`.jar` file]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/kcliqueexample_2.11-0.1.jar) [[Command]](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/kclique.sh))

##### Usage
```bash
# General case
python cli.py perf-eval --platform <platform> --algorithm <algorithm> --path <path_to_the_dataset_folder>

# GraphX requires extra argument
python cli.py perf-eval --platform graphx --algorithm <algorithm>   --path <path_to_the_dataset_folder> --spark-master <spark-master>
```

##### Examples
```bash
# Run PageRank on Flash
python cli.py perf-eval --platform flash --algorithm pagerank --path /data/

# Run Triangle Counting on GraphX
python cli.py perf-eval --platform graphx --algorithm triangle --path /data/ --spark-master spark://spark-master:7077
```

---

## Datasets & Formats

- **Flash**:
  - The dataset is organized in folders named according to the following patterns:
     - For the **sssp** algorithm: `flash-sssp-edges-{SCALE}-{FEATURE}` (e.g., `flash-sssp-edges-8-Standard`)
     - For other algorithms: `flash-edges-{SCALE}-{FEATURE}` (e.g., `flash-edges-8-Standard`)
  
  - Each folder contains the following files:
     - `graph.txt`: The graph data in text format.
     - `graph.idx`: The index file for the graph data.
     - `graph.dat`: The data file for the graph.

  - Download:
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
    
- **Ligra**:
  - The dataset for Ligra is provided as the `.txt` format.
     - For the **sssp** algorithm: `ligra-sssp-adj-{SCALE}-{FEATURE}.txt` (e.g., `ligra-sssp-adj-8-Diameter.txt`)
     - For other algorithms: `ligra-adj-{SCALE}-{FEATURE}` (e.g., `ligra-adj-8-Diameter.txt`)

  - Download:
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
    
- **Grape**: 
   - Grape uses a simple vertex/edge list format, typically stored in two separate files.
       - Vertex File: A file with a .v extension, where each line represents a vertex ID.
         - Format: for the **sssp** algorithm: `grape-sssp-edges-{SCALE}-{FEATURE}.v` (e.g., `grape-sssp-edges-8-Standard.v`); and for other algorithm: `grape-edges-{SCALE}-{FEATURE}.v` (e.g., `grape-edges-8-Standard.v`).
       - Edge File: A file with a .e extension, where each line represents a directed edge (and optionally, a weight).
         - Format: for the **sssp** algorithm: `grape-sssp-edges-{SCALE}-{FEATURE}.e` (e.g., `grape-sssp-edges-8-Standard.e`); and for other algorithm: `grape-edges-{SCALE}-{FEATURE}.e` (e.g., `grape-edges-8-Standard.e`).
      
    - Download:
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

- **Pregel+**: 
   - The dataset for Pregel+ is provided as the `.txt` format.
       - Format: `pregel+-adj-{SCALE}-{FEATURE}.txt` (e.g., `pregel+-adj-8-Standard.txt`)
         
   - Download:
      - [pregel+-adj-8-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-8-Standard.txt)
      - [pregel+-adj-9-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-9-Standard.txt)
      - [pregel+-adj-8-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-8-Density.txt)
      - [pregel+-adj-9-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-9-Density.txt)
      - [pregel+-adj-8-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-8-Diameter.txt)
      - [pregel+-adj-9-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/pregel+-adj-9-Diameter.txt)
        
- **Gthinker**: same as Pregel+. Modify all `pregel+-adj-*` files to `gthinker-adj-*`.
- **PowerGraph**:
  - The dataset for PowerGraph is provided as the `.txt` format.
    - Format: `graphlab-adj-{SCALE}-{FEATURE}.txt` (e.g., `graphlab-adj-8-Standard.txt`)
      
  - Download:
      - [graphlab-adj-8-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-8-Standard.txt)
      - [graphlab-adj-9-Standard.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-9-Standard.txt)
      - [graphlab-adj-8-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-8-Density.txt)
      - [graphlab-adj-9-Density.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-9-Density.txt)
      - [graphlab-adj-8-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-8-Diameter.txt)
      - [graphlab-adj-9-Diameter.txt](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/graphlab-adj-9-Diameter.txt)
        
- **GraphX**: 
  - The dataset is organized in folders named according to the following patterns:
     - For the **pagerank** algorithm: `graphx-edges-{SCALE}-{FEATURE}.txt` (e.g., `graphx-edges-8-Standard.txt]`)
     - For other algorithms: `graphx-weight-edges-{SCALE}-{FEATURE}.txt` (e.g., `graphx-weight-edges-8-Standard.txt`)
       
  - Download:
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
