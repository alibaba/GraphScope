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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class KafkaLogReader implements LogReader {

    private static final Logger logger = LoggerFactory.getLogger(KafkaLogReader.class);

    private static final LogEntryDeserializer deSer = new LogEntryDeserializer();
    private Consumer<LogEntry, LogEntry> consumer;
    private Iterator<ConsumerRecord<LogEntry, LogEntry>> iterator;
    private long latestOffset;
    private long nextReadOffset;

    public KafkaLogReader(String servers, AdminClient adminClient, String topicName, int partitionId, long offset)
            throws IOException {
        Map<String, Object> kafkaConfigs = new HashMap<>();
        kafkaConfigs.put("bootstrap.servers", servers);

        TopicPartition partition = new TopicPartition(topicName, partitionId);
        long earliestOffset;
        try {
            earliestOffset = adminClient.listOffsets(Collections.singletonMap(partition, OffsetSpec.earliest()))
                    .partitionResult(partition)
                    .get()
                    .offset();
            this.latestOffset = adminClient.listOffsets(Collections.singletonMap(partition, OffsetSpec.latest()))
                    .partitionResult(partition)
                    .get()
                    .offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
        if (earliestOffset > offset || offset > this.latestOffset) {
            throw new IllegalArgumentException("cannot read from [" + offset + "], earliest offset is [" +
                    earliestOffset + "], latest offset is [" + this.latestOffset + "]");
        }
        this.consumer = new KafkaConsumer<>(kafkaConfigs, deSer, deSer);
        this.consumer.assign(Arrays.asList(partition));
        this.consumer.seek(partition, offset);
        this.nextReadOffset = offset;
        logger.info("reader created. kafka offset range is [" + earliestOffset + "] ~ [" + this.latestOffset + "]");
    }

    @Override
    public ReadLogEntry readNext() {
        if (this.nextReadOffset == this.latestOffset) {
            return null;
        }
        while (this.iterator == null || !this.iterator.hasNext()) {
            ConsumerRecords<LogEntry, LogEntry> consumerRecords = this.consumer.poll(Duration.ofMillis(100L));
            if (consumerRecords == null || consumerRecords.isEmpty()) {
                logger.info("polled nothing from Kafka. nextReadOffset is [" + this.nextReadOffset + "]");
                continue;
            }
            this.iterator = consumerRecords.iterator();
        }
        ConsumerRecord<LogEntry, LogEntry> record = iterator.next();
        this.nextReadOffset = record.offset() + 1;
        LogEntry v = record.value();
        return new ReadLogEntry(record.offset(), v);
    }

    @Override
    public void close() throws IOException {
        this.consumer.close();
    }
}
