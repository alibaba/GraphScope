/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.common.config;

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
