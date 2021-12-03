# Performance

We evaluate the performandce of `grape-jdk` on [LDBC Graph Analytics Benchmark.](https://graphalytics.org/).
In particular, we evaluate the performance with typical graph apps over LDBC XL-size datasets. 

Comparing the performance result of `grape-jdk` with the performance data from native **analytical engine**, we found that the performance gap between **Java app** and **C++ app** has been made narrowed substantially with the help of `grape-jdk`.
In most cases, the performance gap is below *2x*, moreover, when running pagerank, Java app achieves nearly the same performance as C++ apps.

## Evaluation Settings

The evaluation is conducted on a cluster of 4 instances, each of them is equiped with following hardwares.

- CPU: Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz, 96 cores
- Memory: 400GB

As for the dataset, we use 3 XL-size LDBC datasets.

|     dataset    	| num of vertices 	|  num of edges 	| avg degree  	|
|:--------------:	|:---------------:	|:-------------:	|:-----------:	|
| datagen-9_0-fb 	|    12,857,672   	| 1,049,527,226 	|     81.6    	|
|   graph500-26  	|    32,804,978   	| 1,051,922,853 	|     32.1    	|
| com-friendster 	|    65,608,366   	| 1,806,067,135 	|     27.5    	|

## Refered algorithm implementation

- SSSP
    - [Java]()
    - [C++]()
- PageRank
    - [Java]()
    - [C++]()
- WCC
    - [Java]()
    - [C++]()
- BFS
    - [Java]()
    - [C++]()

## Results

### SSSP

We didn't evaluate SSSP on Graph500 since no edge value is available in Graph500

#### SSSP on Datagen

| threads               	| 1     	| 2    	| 4    	| 8    	| 16   	| 32   	| 64   	|
|-----------------------	|-------	|------	|------	|------	|------	|------	|------	|
| C++ time              	| 7.40  	| 3.76 	| 2.14 	| 0.94 	| 0.74 	| 0.67 	| 0.76 	|
| Java time             	| 30    	| 16   	| 9.6  	| 4.39 	| 3.84 	| 2.77 	| 3.22 	|
| Java(+LLVM4JNI) time  	| 13.14 	| 7.9  	| 4.57 	| 2.58 	| 2.18 	| 1.85 	| 2.16 	|

#### SSSP on com-friendster

| threads              	| 1      	| 2      	| 4     	| 8     	| 16    	| 32    	| 64    	|
|----------------------	|--------	|--------	|-------	|-------	|-------	|-------	|-------	|
| C++ time             	| 57.82  	| 32.9   	| 15.78 	| 9.23  	| 8.33  	| 7.67  	| 7.60  	|
| Java time            	| 267.31 	| 138.85 	| 76.32 	| 45.34 	| 30.8  	| 25.32 	| 31.33 	|
| Java(+LLVM4JNI) time 	| 104.83 	| 59.1   	| 35.88 	| 21.34 	| 15.90 	| 11.39 	| 12.23 	|

### PageRank

pr_delta set to 0.85, running for 50 rounds.

### PageRank on Graph500

| threads              	| 1    	| 2      	| 4     	| 8     	| 16    	| 32    	| 64    	|
|----------------------	|------	|--------	|-------	|-------	|-------	|-------	|-------	|
| C++(s) time          	| 262  	| 158.15 	| 83.15 	| 38.11 	| 25.36 	| 23.60 	| 21.48 	|
| Java(s) time         	| 1602 	| 760    	| 474   	| 225   	| 110   	| 70    	| 83    	|
| Java(+LLVM4JNI) time 	| 344  	| 172.54 	| 93.28 	| 48.56 	| 34.83 	| 25.95 	| 23.42 	|

### PageRank on Datagen-9_0

| threads              	| 1    	| 2   	| 4   	| 8   	| 16 	| 32 	| 64 	|
|----------------------	|------	|-----	|-----	|-----	|----	|----	|----	|
| C++ time             	| 253  	| 113 	| 65  	| 33  	| 22 	| 17 	| 17 	|
| Java time            	| 1439 	| 770 	| 358 	| 162 	| 85 	| 64 	| 74 	|
| Java(+LLVM4JNI) time 	| 393  	| 172 	| 85  	| 41  	| 26 	| 23 	| 22 	|

### PageRank on Com-friendster

| threads              	| 1    	| 2    	| 4   	| 8   	| 16  	| 32  	| 64  	|
|----------------------	|------	|------	|-----	|-----	|-----	|-----	|-----	|
| C++ time             	| 567  	| 325  	| 149 	| 78  	| 39  	| 37  	| 43  	|
| Java time            	| 3166 	| 1651 	| 621 	| 373 	| 197 	| 107 	| 147 	|
| Java(+LLVM4JNI) time 	| 743  	| 377  	| 202 	| 99  	| 53  	| 38  	| 48  	|

### WCC

### WCC on Graph500
| threads              	| 1     	| 2     	| 4     	| 8     	| 16   	| 32   	| 64   	|
|----------------------	|-------	|-------	|-------	|-------	|------	|------	|------	|
| C++ time             	| 28.9  	| 13.6  	| 7.20  	| 3.84  	| 2.15 	| 1.73 	| 1.67 	|
| Java time            	| 93.78 	| 52.13 	| 30.7  	| 19.55 	| 7.66 	| 4.53 	| 5.19 	|
| Java(+LLVM4JNI) time 	| 61.2  	| 32.71 	| 15.32 	| 8.06  	| 5.06 	| 3.46 	| 3.62 	|

### WCC on Datagen-9_0

| threads              	| 1     	| 2     	| 4     	| 8     	| 16   	| 32   	| 64   	|
|----------------------	|-------	|-------	|-------	|-------	|------	|------	|------	|
| C++ time             	| 23.86 	| 16.11 	| 7.85  	| 4.67  	| 2.15 	| 1.32 	| 1.24 	|
| Java time            	| 66.15 	| 33    	| 21.7  	| 10.84 	| 6.50 	| 3.51 	| 3.81 	|
| Java(+LLVM4JNI) time 	| 37    	| 17.21 	| 10.24 	| 5.32  	| 2.90 	| 1.90 	| 2.30 	|

### WCC on Com-friendster

| threads              	| 1      	| 2    	| 4     	| 8     	| 16    	| 32   	| 64   	|
|----------------------	|--------	|------	|-------	|-------	|-------	|------	|------	|
| C++ time             	| 60.3   	| 28.9 	| 15.2  	| 8.3   	| 5.12  	| 4.60 	| 7.41 	|
| Java time            	| 192.6  	| 95.5 	| 48.7  	| 26.1  	| 14.14 	| 9.11 	| 9.56 	|
| Java(+LLVM4JNI) time 	| 126.13 	| 68.4 	| 34.78 	| 21.67 	| 12.90 	| 8.35 	| 7.70 	|

### BFS

### BFS on Graph500

| threads              	| 1    	| 2    	| 4    	| 8    	| 16   	| 32   	| 64   	|
|----------------------	|------	|------	|------	|------	|------	|------	|------	|
| C++ time             	| 1.73 	| 0.94 	| 0.68 	| 0.56 	| 0.39 	| 0.36 	| 0.50 	|
| Java time            	| 2.63 	| 1.44 	| 1.10 	| 0.76 	| 0.60 	| 0.59 	| 0.58 	|
| Java(+LLVM4JNI) time 	| 2.60 	| 1.28 	| 1.04 	| 0.69 	| 0.58 	| 0.57 	| 0.57 	|

### BFS on Datagen-9_0

| threads              	| 1     	| 2    	| 4    	| 8    	| 16   	| 32   	| 64   	|
|----------------------	|-------	|------	|------	|------	|------	|------	|------	|
| C++ time             	| 5.33  	| 2.01 	| 1.10 	| 0.87 	| 0.63 	| 0.45 	| 0.47 	|
| Java time            	| 14.87 	| 6.60 	| 3.76 	| 1.66 	| 1.28 	| 0.90 	| 0.93 	|
| Java(+LLVM4JNI) time 	| 8.75  	| 4.12 	| 2.16 	| 1.28 	| 1.01 	| 0.78 	| 0.81 	|

### BFS on Com-friendster

| threads              	| 1     	| 2     	| 4     	| 8     	| 16   	| 32   	| 64   	|
|----------------------	|-------	|-------	|-------	|-------	|------	|------	|------	|
| C++ time             	| 24.15 	| 12.46 	| 6.59  	| 3.59  	| 2.11 	| 1.56 	| 1.53 	|
| Java time            	| 80.77 	| 40.94 	| 20.87 	| 14.55 	| 8.14 	| 5.13 	| 5.15 	|
| Java(+LLVM4JNI) time 	| 49.80 	| 24.15 	| 10.54 	| 6.63  	| 3.83 	| 2.95 	| 3.42 	|
