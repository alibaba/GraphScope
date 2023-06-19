# Performance

We test GraphScope for GraphX in end-to-end scenarios to measure the performance improvement of graph computing on Spark GraphX. This includes:
- Graph loading: loading graphs from the file system into memory in the form of a graph
- RDD Op: transforming the graph using RDD-defined operators
- Pregel computin: running graph algorithms based on GraphX Pregel, such as SSSP, PageRank, and CC

##  Settings:

|     dataset    	| num of vertices 	|  num of edges 	| avg degree  	|
|:--------------:	|:---------------:	|:-------------:	|:-----------:	|
| datagen-9_0-fb 	|    12,857,672   	| 1,049,527,226 	|     81.6    	|
| com-friendster 	|    65,608,366   	| 1,806,067,135 	|     27.5    	|

The following tests are run on 4 Nodes cluster, each with 48 cores, 96 cpu.


## End-to-End time

By using ORC-format files as input, the time for graph loading and converting it to ```RDD[(Long, Long)]``` is the same for GraphScope and GraphX.

### On com-friendster
#### 256 partitions

| Algorithm | GS Graph Loading | GraphX Graph Loading | GS Query Time | GraphX Query Time  | GS E2E Time| GraphX E2E Time | Performance Gain Query | Performance Gain E2E |
|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------: |
|PageRank | 108s          | 106s       | 152s           | 1129s            | 260s | 1235s  | 7.4x  | 4.8x |
| SSSP | 108s          | 106s       | 31s            | 164s             | 139s | 270s   | 5.3   | 1.9x |
| CC | 108s          | 106s       | 58s            | 228s             | 166s | 334s   | 3.9x  | 2x   |


#### 320 partitions

| Algorithm | GS Graph Loading | GraphX Graph Loading | GS Query Time | GraphX Query Time  | GS E2E Time| GraphX E2E Time | Performance Gain Query | Performance Gain E2d |
|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------: |
| PageRank | 100s | 100s | 158s | 1089s | 268s | 1189s | 6.5x | 4.4x |
| SSSP     | 100s | 100s | 31s  | 156s  | 131s | 256s  | 5x   | 2x   |
| CC       | 100s | 100s | 62s  | 219s  | 162s | 319s  | 2.8x | 2x   |

#### 384 partitions
| Algorithm | GS Graph Loading | GraphX Graph Loading | GS Query Time | GraphX Query Time  | GS E2E Time| GraphX E2E Time | Performance Gain Query | Performance Gain E2d |
|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------: |
| PageRank | 99s | 98s | 154s | 1028s | 253s | 1126s | 6.7x | 4.5x |
| SSSP     | 99s | 98s | 33s  | 163s  | 132s | 261s  | 5x   | 2x   |
| CC       | 99s | 98s | 60s  | 223s  | 159s | 321s  | 2.8x | 2x   |



### On Datagen-9_0-fb

#### 256 partitions

| Algorithm | GS Graph Loading | GraphX Graph Loading | GS Query Time | GraphX Query Time  | GS E2E Time| GraphX E2E Time | Performance Gain Query | Performance Gain E2d |
|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------: |
| PageRank | 70s | 84s | 90s | 430s | 160s | 514s | 4.8x | 3.2x |
| SSSP     | 70s | 84s | 14s | 45s  | 84s  | 129s | 3x   | 1.5x |
| CC       | 70s | 84s | 36s | 74s  | 106s | 158s | 2x   | 1.5x |


#### 320 partitions

| Algorithm | GS Graph Loading | GraphX Graph Loading | GS Query Time | GraphX Query Time  | GS E2E Time| GraphX E2E Time | Performance Gain Query | Performance Gain E2d |
|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------: |
| PageRank | 68s | 76s | 87s | 406s | 155s | 482s | 4.7x | 3.1x |
| SSSP     | 68s | 76s | 13s | 40s  | 81s  | 116s | 3x   | 1.4x |
| CC       | 68s | 76s | 30s | 53s  | 98s  | 129s | 1.8x | 1.3x |

#### 384 partitions

| Algorithm | GS Graph Loading | GraphX Graph Loading | GS Query Time | GraphX Query Time  | GS E2E Time| GraphX E2E Time | Performance Gain Query | Performance Gain E2d |
|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------:|:--------------: |
| PageRank | 68s | 73s | 82s | 395s | 150s | 468s | 4.8x | 3x   |
| SSSP     | 68s | 73s | 13s | 40s  | 81s  | 113s | 3x   | 1.4x |
| CC       | 68s | 73s | 30s | 50s  | 98s  | 143s | 1.7x | 1.4x |
