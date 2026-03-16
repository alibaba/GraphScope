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

- **Python 3.x+** (recommended: 3.10/3.11)  
- **Docker** (required for `llm-eval` and platform images)  
- **Kubernetes + MPI Operator + Hadoop** (for distributed experiments)  
- (GraphX only) **Java 8 / Scala 2.11 / Spark 2.4.x (Hadoop 2.7)**

### Global help
```bash
python3 gab_cli.py --help
```

### `datagen` Data Generation

#### Requirements
We provide a lightweight C++ program (download from [Data_Generator.zip](https://graphscope.oss-cn-beijing.aliyuncs.com/benchmark_datasets/Data_Generator.zip)) to generate data. Download and unzip to the `Data_Generator/` folder.

#### Usage
```bash
python3 gab_cli.py datagen --platform <platform> --scale <scale> --feature <feature>
```
- **--platform**: Target graph system (e.g., `flash`, `ligra`, `grape`, `gthinker`, `pregel+`, `powergraph`, `graphx`)
- **--scale**: Graph scale (e.g., `8`, `9`, `10`, or custom value)
- **--feature**: Data generation feature (`Standard`, `Density`, or `Diameter`)

#### Example
```bash
python3 gab_cli.py datagen --scale 9 --platform flash --feature Standard
```
---

### `llm-eval` LLM Usability Evaluation

#### Requirements
- `OPENAI_API_KEY` set in `.env.example` or system environment.

#### Usage
```bash
python3 gab_cli.py llm-eval --platform <platform> --algorithm <algorithm>
```
- **--platform**: Target graph system (e.g., `flash`, `ligra`, `grape`, `gthinker`, `pregel+`, `powergraph`, `graphx`)
- **--algorithm**: Algorithm to evaluate usability (e.g., `pagerank`, `sssp`, `triangle`, `bc`, `cd`, `lpa`, `kclique`, `cc`)

#### Example
```bash
python3 gab_cli.py llm-eval --platform flash --algorithm pagerank
```

---

### `perf-eval` Performance Evaluation

#### Requirements
- All performance experiments are conducted in a properly configured **Kubernetes + MPI Operator (MPIJob) + Hadoop** environment to ensure reproducibility and consistency.  
- **GraphX** experiments additionally require a properly configured **Spark environment** (Spark 2.4.x, Scala 2.11, Hadoop 2.7, Java 8).

#### Usage
```bash
python3 gab_cli.py perf-eval --platform <platform> --algorithm <algorithm> [--path <dataset_file>] [--spark-master <spark-master>]
```
- **--platform**: Target graph system (e.g., `flash`, `ligra`, `grape`, `gthinker`, `pregel+`, `powergraph`, `graphx`)
- **--algorithm**: Algorithm to run (e.g., `pagerank`, `sssp`, `triangle`, `bc`, `cd`, `lpa`, `kclique`, `cc`)
- **--path**: Path to input dataset file or directory.  
  > **All machines in the cluster must have the dataset available at the same path.**  
  > If not specified, a default test dataset will be used.

  > **Note:**  
  > - For **flash**, specify a **directory** containing the dataset files.  
  > - For **grape**, provide the **prefix** for `.e` and `.v` files.  
  > - For other platforms, specify the **complete dataset file**.  
  > See the [sample datasets]() for details.
- **--spark-master**: Spark master URL (only needed for **GraphX** experiments)

#### Examples
```bash
# Run PageRank on Flash
python3 gab_cli.py perf-eval --platform flash --algorithm pagerank --path sample_data/flash_sample_graph/

# Run PageRank on Grape
python3 gab_cli.py perf-eval --platform grape --algorithm pagerank --path sample_data/grape_sample_graph

# Run PageRank on Ligra
python3 gab_cli.py perf-eval --platform ligra --algorithm pagerank --path sample_data/ligra_sample_graph.txt

# Run Triangle Counting on GraphX
python3 gab_cli.py perf-eval --platform graphx --algorithm triangle --path sample_data/graphx_sample_graph.txt --spark-master spark://spark-master:7077
```
---     


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
