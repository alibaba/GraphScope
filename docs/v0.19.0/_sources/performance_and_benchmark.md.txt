# Performance and Benchmark
We evaluated performance of GraphScope with the benchmarks provided by [Linked Data Benchmark Council (LDBC)](https://ldbcouncil.org/). Specifically, we evaluated **Graph Analytical Engine (GAE)** with [LDBC Graph Analytics Benchmark](http://graphalytics.org/), which is an industrial-grade benchmark that enables the objective comparison of graph analysis platforms. We also evaluated **Graph Interactive Engine (GIE)** with LDBC Social Network Benchmark, which focuses on graph database management systems and includes two workloads for interactive transactional queries and analytical queries. We compared GraphScope's performance to the state-of-the-art systems including PowerGraph, GeminiGraph, Plato, Neo4j, etc. and found GraphScope achieves high performance in most cases.

## Graph Analytical Engine
We evaluated performance of **Graph Analytical Engine** (**GAE**, in short in the following) with [LDBC Graph Analytics Benchmark](http://graphalytics.org/). In addition to the ease of programming, we find that **GAE** achieves high performance comparably to the state-of-the-art systems. The experiments were conducted on 4 instances of [r6.8xlarge](https://www.alibabacloud.com/help/doc-detail/25378.htm#d12e563) on [AlibabaCloud ECS](https://www.alibabacloud.com/product/ecs), each with 32 threads, over LDBC XL-size datasets. Instances are imaged with [Aliyun Linux (a CentOS-variant)](https://www.alibabacloud.com/help/doc-detail/111881.htm). The datasets can be downloaded from [here](https://ldbcouncil.org/benchmarks/graphalytics/).

We compared **GAE** with [PowerGraph](https://github.com/jegonzal/PowerGraph)(commit a038f97
) [GeminiGraph](https://github.com/thu-pacman/GeminiGraph)(commit 170e7d3
) and [Plato](https://github.com/Tencent/plato)(commit 21009d6). Each system is built with GCC(v4.8.5) and MPICH(v3.1). To make the comparisons fair, **GAE** was built with HUGE_PAGES and jemalloc disabled.

We made minor changes on their code and datasets:
- Turned on `-O3` optimization for all three systems.
- Added timing stubs for Plato.
- Replaced the random-pick logic with the deterministic logic from LDBC for the CDLP in Plato.
- Changed the weight type of SSSP from `float` to `double` in GeminiGraph.
- Changed the load strategy from `load_directed` to `load_undirected_from_directed` for PageRank in GeminiGraph
- Reformat the datasets to adapt Plato and GeminiGraph's formats (e.g., 0-based continuous vertex ids)

### Results
The results are reported below. The numbers in the table represent the evaluation time in seconds. 
The best results are marked in **bold**.

| Algorithm | Dataset        | PowerGraph | GeminiGraph | Plato | GAE |
|-----------|----------------|------------|-------------|-------|---------------|
| SSSP      | datagen-9_0-fb | 5.08       | 0.62        | N/A   | **0.42**      |
|           | datagen-9_1-fb | 5.30       | 0.78        | N/A   | **0.56**      |
|           | datagen-9_2-zf | 41.19      | 3.75        | N/A   | **1.48**      |
| WCC       | datagen-9_0-fb | 14.14      | 0.88        | 2.60  | **0.41**      |
|           | datagen-9_1-fb | 18.61      | 1.17        | 3.07  | **0.50**      |
|           | datagen-9_2-zf | 176.87     | 6.26        | 25.49 | **1.32**      |
|           | graph500-26    | 13.71      | 1.60        | 4.79  | **0.71**      |
|           | com-friendster | 44.20      | 3.97        | 7.80  | **1.97**      |
| BFS       | datagen-9_0-fb | 3.90       | 0.24        | 0.59  | **0.07**      |
|           | datagen-9_1-fb | 4.30       | 0.28        | 0.71  | **0.13**      |
|           | datagen-9_2-zf | 39.11      | 1.97        | 10.37 | **1.16**      |
|           | graph500-26    | 4.86       | 0.53        | 1.56  | **0.20**      |
|           | com-friendster | 12.80      | 1.09        | 2.67  | **0.74**      |
| PageRank  | datagen-9_0-fb | 22.57      | X           | X     | **1.40**      |
|           | datagen-9_1-fb | 28.38      | X           | X     | **1.73**      |
|           | datagen-9_2-zf | 126.98     | X           | X     | **3.83**      |
|           | graph500-26    | 28.66      | X           | X     | **2.42**      |
|           | com-friendster | 57.10      | X           | X     | **6.04**      |
| CDLP      | datagen-9_0-fb | 1695.73    | N/A         | 16.30 | **8.18**      |
|           | datagen-9_1-fb | 2742.47    | N/A         | 21.35 | **10.40**     |
|           | datagen-9_2-zf | > 3600     | N/A         | 34.85 | **19.48**     |
|           | graph500-26    | 425.55     | N/A         | 12.86 | **7.59**      |
|           | com-friendster | > 3600     | N/A         | 36.87 | **19.10**     |
| LCC       | datagen-9_0-fb | 521.26     | N/A         | N/A   | **14.51**     |
|           | datagen-9_1-fb | 600.32     | N/A         | N/A   | **18.35**     |
|           | datagen-9_2-zf | 296.18     | N/A         | N/A   | **8.98**      |
|           | graph500-26    | 1859.86    | N/A         | N/A   | **201.20**    |
|           | com-friendster | 842.68     | N/A         | N/A   | **61.44**     |


We used “default” code provided by the competitor systems when it is available. 
- **N/A** indicate that the system didn't provide the application. And
- **X**  indicates the results produced are not consistent with the verified results provided by LDBC.

The inconsistences of PageRank come from different settings on convergence conditions. 
To give a comprehensive comparison, we made our best efforts to revise our application([pagerank_local.h](examples/analytical_apps/pagerank/pagerank_local.h)), making them output the same results as competitor systems.
The performance results are shown as below. 

| Algorithm | Dataset        | GeminiGraph | Plato | GAE |
|-----------|----------------|-------------|-------|---------------|
| PageRank  | datagen-9_0-fb | 2.21        | 4.65  | **1.39**      |
|           | datagen-9_1-fb | 2.72        | 5.38  | **1.73**      |
|           | datagen-9_2-zf | 7.84        | 36.11 | **3.63**      |
|           | graph500-26    | 4.75        | 12.25 | **2.34**      |
|           | com-friendster | 8.19        | 15.82 | **5.84**      |

### Performance on GPUs
We compare the GAE GPU version with [gunrock](https://github.com/gunrock/gunrock)(commit 0c9a96, tag:v0.5.1) and [groute](https://github.com/groute/groute)(commit 5ce160).
Each system is built with GCC(v7.3.0), OpenMPI(v4.1.1), and CUDA(v10.0). 
The experiments were conducted on GPU instances of [gn6v-c8g1.16xlarge](https://www.alibabacloud.com/help/zh/doc-detail/25378.htm#gn6v) on [AlibabaCloud ECS](https://www.alibabacloud.com/product/ecs), 
equipped with 8x 16GB NVIDIA-V100 GPU, and 256 GB host memory.
Instances are imaged with [Aliyun Linux (a CentOS-variant)](https://www.alibabacloud.com/help/doc-detail/111881.htm).
Since the device memory of GPUs is limited, we conducted the comparison on several smaller graphs from the paper of Gunrock and Groute.

The results are reported below. The numbers in the table represent the evaluation time in milliseconds. 
The best results are marked in **bold**.

| Algorithm   | Dataset          | Gunrock   | Groute   | GAE-GPU |
|-------------|------------------|-----------|----------|-------------------|
| SSSP        | soc-LiveJournal1 |   89.9345 | 215.560  |        **32.3029**|
|             | soc-twitter-2010 |  129.454  | 389.155  |        **88.0549**|
|             | soc-sinaweibo    |  210.168  | 1649.647 |         **172.03**|
|             | soc-orkut        |  105.121  | 193.658  |        **54.3201**|
| WCC         | soc-LiveJournal1 |   50.7677 | 12.137   |        **10.2139**|
|             | soc-twitter-2010 |  319.261  | 69.235   |         **66.334**|
|             | soc-sinaweibo    |  567.09   | 283.073  |        **68.6619**|
|             | soc-orkut        |   45.777  | **7.150**|            8.87299|
| BFS         | soc-LiveJournal1 |   28.162  | 51.000   |        **5.60403**|
|             | soc-twitter-2010 |   45.9676 | 141.347  |        **27.7772**|
|             | soc-sinaweibo    |   55.0118 | 168.306  |        **34.8618**|
|             | soc-orkut        |   41.9134 | 37.467   |        **6.60801**|
| PageRank    | soc-LiveJournal1 |   39.8666 | 3501.870 |        **24.6451**|
|             | soc-twitter-2010 |  198.829  | 5036.229 |        **127.024**|
|             | soc-sinaweibo    |  483.178  | 5593.807 |        **228.367**|
|             | soc-orkut        |   57.6946 | N/A      |        **49.2339**|

### Reproducing the results

We will release a public image containing the script, together with all the systems and datasets on AlibabaCloud and AWS soon.

## Graph Interactive Engine
TBF
