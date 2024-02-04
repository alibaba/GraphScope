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
package com.alibaba.graphscope.groot.wal.kafka;

import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogReader;
import com.alibaba.graphscope.groot.wal.ReadLogEntry;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaLogReader implements LogReader {

    private static final Logger logger = LoggerFactory.getLogger(KafkaLogReader.class);

    private static final LogEntryDeserializer deSer = new LogEntryDeserializer();
    private final Consumer<LogEntry, LogEntry> consumer;
    private Iterator<ConsumerRecord<LogEntry, LogEntry>> iterator;
    private final long latest;
    private long nextReadOffset;

    public KafkaLogReader(
            String servers,
            AdminClient client,
            String topicName,
            int partitionId,
            long offset,
            long timestamp)
            throws IOException {
        Map<String, Object> kafkaConfigs = new HashMap<>();
        kafkaConfigs.put("bootstrap.servers", servers);

        TopicPartition partition = new TopicPartition(topicName, partitionId);

        long earliest = getOffset(client, partition, OffsetSpec.earliest());
        latest = getOffset(client, partition, OffsetSpec.latest());

        if (offset == -1 && timestamp == -1) { // Seek to end
            offset = latest;
        } else if (offset == -1) { // Get offset from timestamp
            offset = getOffset(client, partition, OffsetSpec.forTimestamp(timestamp));
        }
        if (earliest > offset || offset > latest) {
            throw new IllegalArgumentException(
                    "invalid offset " + offset + ", hint: [" + earliest + ", " + latest + ")");
        }
        consumer = new KafkaConsumer<>(kafkaConfigs, deSer, deSer);

        consumer.assign(List.of(partition));
        consumer.seek(partition, offset);
        nextReadOffset = offset;
        logger.info(
                "reader created with offset [{}], offset range is [{}] ~ [{}]",
                offset,
                earliest,
                latest);
    }

    private long getOffset(AdminClient client, TopicPartition partition, OffsetSpec spec)
            throws IOException {
        try {
            return client.listOffsets(Collections.singletonMap(partition, spec))
                    .partitionResult(partition)
                    .get()
                    .offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ReadLogEntry readNext() {
        if (nextReadOffset == latest) {
            return null;
        }
        while (iterator == null || !iterator.hasNext()) {
            ConsumerRecords<LogEntry, LogEntry> consumerRecords =
                    consumer.poll(Duration.ofMillis(100L));
            if (consumerRecords == null || consumerRecords.isEmpty()) {
                logger.info("polled nothing from Kafka. nextReadOffset is [{}]", nextReadOffset);
                continue;
            }
            iterator = consumerRecords.iterator();
        }
        ConsumerRecord<LogEntry, LogEntry> record = iterator.next();
        nextReadOffset = record.offset() + 1;
        LogEntry v = record.value();
        return new ReadLogEntry(record.offset(), v);
    }

    @Override
    public ConsumerRecord<LogEntry, LogEntry> readNextRecord() {
        if (nextReadOffset == latest) {
            return null;
        }
        while (iterator == null || !iterator.hasNext()) {
            ConsumerRecords<LogEntry, LogEntry> consumerRecords =
                    consumer.poll(Duration.ofMillis(100L));
            if (consumerRecords == null || consumerRecords.isEmpty()) {
                logger.info("polled nothing from Kafka. nextReadOffset is [{}]", nextReadOffset);
                continue;
            }
            iterator = consumerRecords.iterator();
        }
        ConsumerRecord<LogEntry, LogEntry> record = iterator.next();
        nextReadOffset = record.offset() + 1;
        return record;
    }

    public ConsumerRecords<LogEntry, LogEntry> getLatestUpdates() {
        return consumer.poll(Duration.ofMillis(1000L));
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
