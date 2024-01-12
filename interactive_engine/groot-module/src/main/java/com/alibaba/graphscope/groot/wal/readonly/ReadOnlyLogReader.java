/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.wal.readonly;

import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.ReadLogEntry;
import com.alibaba.graphscope.groot.wal.kafka.LogEntryDeserializer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class ReadOnlyLogReader implements LogReader {

    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyLogReader.class);

    private static final LogEntryDeserializer deSer = new LogEntryDeserializer();
    private final Consumer<LogEntry, LogEntry> consumer;

    public ReadOnlyLogReader(String servers, String topicName, int partitionId) throws IOException {
        Map<String, Object> kafkaConfigs = new HashMap<>();
        kafkaConfigs.put("bootstrap.servers", servers);

        TopicPartition partition = new TopicPartition(topicName, partitionId);

        consumer = new KafkaConsumer<>(kafkaConfigs, deSer, deSer);
        consumer.assign(List.of(partition));
        consumer.seekToEnd(consumer.assignment());
        logger.info("Created MockLogReader");
    }

    public ConsumerRecords<LogEntry, LogEntry> getLatestUpdates() {
        ConsumerRecords<LogEntry, LogEntry> consumerRecords =
                consumer.poll(Duration.ofMillis(1000L));
        return consumerRecords;
    }

    @Override
    public ReadLogEntry readNext() {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
