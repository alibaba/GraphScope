## Pegasus 

Pegasus is a distributed data-parallel compute engine, use the cyclic dataflow computational model; Users can constructor their own DCG job, 
and run the job on their laptop or a distributed environment accross a cluster of computers;

## Cargo dependency
### The first way: 
1. Add the public ssh key to you gitlab at profile settings;
2. Enable ssh-agent, more details in [ssh-agent](https://www.ssh.com/ssh/agent);
3. Add git dependency in the 'Cargo.toml' : 

```toml
pegasus = { git = "ssh://git@gitlab.alibaba-inc.com/biggraph/Pegasus.git", branch = "develop" }
``` 
### The second way: 

1) edit the cargo config file(usually in `~/.cargo/config`), add line: 

```
[net]
git-fetch-with-cli = true
```
2)  Add git dependency in the 'Cargo.toml': 

```toml
pegasus = { git = "http://gitlab.alibaba-inc.com/biggraph/Pegasus.git", branch = "develop" }
```

## Examples(maybe departed)

### K-hop

The `k-hop` is a graph traversal job, which start from a source vertex, and do traversal along edges to get neighbours; 

Find source code in [examples/k-hop](http://gitlab.alibaba-inc.com/biggraph/Pegasus/blob/develop/pegasus/examples/k_hop.rs); 

#### Run on laptop

You can run with command: 

```bash
cargo run --example k_hop 2 ./graph/resources/data/facebook_combined.txt -w 2
```

It load a social network graph from facebook: [ego-Facebook](http://snap.stanford.edu/data/ego-Facebook.html);

You can also load your own graph by replace the second parameter after 'k_hop' of to specific another path, make sure the file format is same;

(You can get a binary graph file through [examples/convert](http://gitlab.alibaba-inc.com/biggraph/Pegasus/blob/develop/graph/examples/convert.rs) if the file format is really different);

The program will count 2-hop neighbors for each starting vertices which is sampled from the graph; You can set the `K_HOP_SAMPLES` 
environment variables to change the numbers of starting vertices(default 100) with: 

```bash
export K_HOP_SAMPLES=1000
```


#### Run as service in distributed 

You can also run k-hop on a cluster consist of many servers by provide a hosts file, For example, with two servers: 

Prepare a 'hostfile' file  that specific the addresses of servers, like: 

```
host0:port
host1:port
```

And then compile the 'k_hop.rs' to a executable file and dispatch it to all peers;
```bash
cargo build --example k_hop --release
```

After dispatch,  on the first server, type in command: 

```bash
./target/release/examples/k_hop 3 ./graph/resources/data/facebook_combined.txt -w 2 -p 0 -n 2 -h hostfile
```

On the second server, type in command: 

```bash
./target/release/examples/k_hop 3 ./graph/resources/data/facebook_combined.txt -w 2 -p 1 -n 2 -h hostfile
```

## Other docs:
[Trace system]( http://gitlab.alibaba-inc.com/biggraph/Pegasus/wikis/Trace_System)





