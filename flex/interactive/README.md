# GraphScope Interactive

GraphScope Interactive is a specialized construction of [GraphScope Flex](https://github.com/alibaba/GraphScope/tree/main/flex), designed to handle concurrent graph queries at an impressive speed. Its primary goal is to process as many queries as possible within a given timeframe, emphasizing a high query throughput rate.
For the full documentation of GraphScope Interactive, please refer to [GraphScope Interactive Documentation](https://graphscope.io/docs/latest/flex/interactive_intro).


## Master Slave Replication

### Download kafka

```bash
wget https://dlcdn.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0
```

### Start kafka with Kraft mode

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```

### Create topics

```bash
bin/kafka-topics.sh --create --topic graph-0-wal --bootstrap-server localhost:9092
```

### Write events to topic in c++