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

import com.alibaba.graphscope.groot.common.exception.GrootException;
import com.alibaba.graphscope.groot.wal.LogEntry;
import com.alibaba.graphscope.groot.wal.LogWriter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaLogWriter implements LogWriter {
    private static final Logger logger = LoggerFactory.getLogger(KafkaLogWriter.class);

    private static final LogEntrySerializer ser = new LogEntrySerializer();
    private final Producer<LogEntry, LogEntry> producer;
    private final String topicName;

    public KafkaLogWriter(String servers, String topicName, Map<String, String> customConfigs) {
        this.topicName = topicName;

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("bootstrap.servers", servers);
        producerConfig.put("enable.idempotence", true);
        producerConfig.put("max.request.size", 1048576000);

        customConfigs.forEach(
                (k, v) -> {
                    producerConfig.put(k, v);
                });
        this.producer = new KafkaProducer<>(producerConfig, ser, ser);
    }

    public long append(int partition, LogEntry logEntry) {
        Future<RecordMetadata> future =
                producer.send(new ProducerRecord<>(topicName, partition, null, logEntry));
        return waitFuture(future);
    }

    @Override
    public long append(LogEntry logEntry) {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topicName, logEntry));
        return waitFuture(future);
    }

    @Override
    public Future<RecordMetadata> appendAsync(int partition, LogEntry logEntry) {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topicName, logEntry));
        return future;
    }

    public long waitFuture(Future<RecordMetadata> future) {
        RecordMetadata recordMetadata;
        try {
            recordMetadata = future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("append kafka failed", e);
            throw new GrootException(e);
        }
        return recordMetadata.offset();
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }
}
