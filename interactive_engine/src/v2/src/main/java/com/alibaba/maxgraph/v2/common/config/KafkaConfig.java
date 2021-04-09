package com.alibaba.maxgraph.v2.common.config;

public class KafkaConfig {
    public static final Config<String> KAFKA_SERVERS =
            Config.stringConfig("kafka.servers", "localhost:9092");

    public static final Config<String> KAKFA_TOPIC =
            Config.stringConfig("kafka.topic", "maxgraph");

    public static final Config<Short> KAFKA_REPLICATION_FACTOR =
            Config.shortConfig("kafka.replication.factor", (short) 1);

    public static final Config<String> KAFKA_PRODUCER_CUSTOM_CONFIGS =
            Config.stringConfig("kafka.producer.custom.configs", "");
}
