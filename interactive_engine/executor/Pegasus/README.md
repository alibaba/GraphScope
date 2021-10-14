## Pegasus 

Pegasus is a distributed data-parallel compute engine, use the cyclic dataflow computational model; Users can constructor their own DCG job, 
and run the job on their laptop or a distributed environment accross a cluster of computers;

## Examples

### K-hop

The `k-hop` is a graph traversal job, which start from a source vertex, and do traversal along edges to get neighbours; 

Find source code in [examples/k-hop](http://gitlab.alibaba-inc.com/biggraph/Pegasus/blob/master/examples/k_hop.rs); 

#### Run on laptop

You can run with command: 

```shell
export RUST_LOG=k_hop=info && cargo run --example k_hop -- -f  ./data/facebook_combined.txt -q ./data/source_vertex.csv
```

It load a social network graph from facebook: [ego-Facebook](http://snap.stanford.edu/data/ego-Facebook.html)， and try to count 3-hop neighbors of the vertexes in `./data/source_vertex.csv`； 

You can load your own graph by change the parameger of `-f` to specific the path of the graph raw file;

 *Note*: After the first load, the program will convert the raw graph file to a binary format file, which will be load faster the next time; For example, it will convert `./data/facebook_combined.txt`  to `./data/facebook_combined.bin` after first execution. Next time you can use a `-b` option to specific it : 

```shell
export RUST_LOG=k_hop=info && cargo run --example k_hop -- -b -f  ./data/facebook_combined.bin -q ./data/source_vertex.csv
```

More k-hop task configuration can be found in the source vertex file after the `-q` option; By default, the file only contains source vertex id like: 

```csv
12
13
14
15
16
```

Each line will submit a k-hop task that start from the source id in the line; You can change the default `3-hop` by append`,k` after each line(k=1,2,3,4,5...); Append`,w` to configure the dataflow parallel degree(w=1,2,3...), by default it is 2, like : 

```csv
12,4,8
13,4,8
14,4,8
15,4,8
16,4,8
```

More task configuration details can be found in the example source file documentation; 

#### Run as service in distributed 

You can config to run `Pegasus` as a service on distrubted servers, which initialize once and accept new task continuously.  For example, you can run k-hop service on two servers: 

Prepare a 'hostfile' file  that specific the addresses of servers, like: 

```
host0:port
host1:port
```

On the first server: 

```shell
export RUST_LOG=k_hop=info && cargo run --example k_hop -- --server -f ./data/facebook_combined.txt -d ./hostfile -p 2 -i 0 -q ''
```

On the second server: 

```shell
export RUST_LOG=k_hop=info && cargo run --example k_hop -- --server -f ./data/facebook_combined.txt -d ./hostfile -p 2 -i 1 -q ''
```

the first server's outut looks like: 

```
2019-xx-xx xx:xx:xx,xxx INFO [main] [examples/k_hop.rs:388] start to load graph ...
finished load graph with 88234 edges, cost 486.615857ms
start service on V4(0.0.0.0:56927)

```

the second server's output looks like: 

```
2019-xx-xx xx:xx:xx,xxx INFO [main] [examples/k_hop.rs:388] start to load graph ...
finished load graph with 88234 edges, cost 456.612451ms
start service on V4(0.0.0.0:56928)
```

Write these service address into a file, e.g. server_address, like: 

```
host0:56927
host1:56928
```

and then use client to submit k-hop tasks: 

```shell
export RUST_LOG=k_hop=info && cargo run --example k_hop -- --client --addr ./server_address -q /data/source_vertex.csv -f ''
```

The service won't exit unless be killed;

You can configure how many servers each task would be submitted to by append `,p` (p=1,2,not large than the server number) to the source vertex file, like: 

```txt
12,4,8,1
13,4,8,2
14,4,8,1
15,4,8,2
16,4,8,1
```



We have evaluate k-hop on multi-graphs with different scale; You can find the benchmark at [k-hop benchmark](http://gitlab.alibaba-inc.com/biggraph/Pegasus/wikis/k_hop_benchmark) 



