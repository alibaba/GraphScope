[zookeeper]
zk = localhost:2181
zk.timeout.ms = 6000

[hadoop]
hadoop.home = /usr/local/hadoop-2.8.4
hdfs.default.fs = hdfs://namenode:9000

[instance]
graph.name = graphname
partition.num = 1
replica.count = 1

[server]
worker.id = 1
worker.num = 1
hb.interval.ms = 5000

[rpc]
grpc.thread.count = 16

[storage]
local.data.root = /home/maxgraph/data/
total.memory.mb = 1024

[bulk-load]
download.thread.count = 4
load.thread.count = 4

[realtime-write]
realtime.write.ingest.count = 1
realtime.write.buffer.size = 1024
realtime.write.buffer.mb = 16
realtime.write.queue.count = 128
realtime.precommit.buffer.size = 8388608

[timely]
timely.worker.per.process = 2 
timely.prepare.dir = prepare_query_info

[vineyard]
graph.type = VINEYARD
graph.vineyard.object.id = VINEYARD_OBJECT_ID
