package com.alibaba.maxgraph.v2.common.wal.kafka;

import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.wal.LogEntry;
import com.alibaba.maxgraph.v2.common.wal.LogWriter;
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
    private Producer<LogEntry, LogEntry> producer;
    private String topicName;
    private int partitionId;

    public KafkaLogWriter(String servers, String topicName, int partitionId, Map<String, String> customConfigs) {
        this.topicName = topicName;
        this.partitionId = partitionId;

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put("bootstrap.servers", servers);
        producerConfig.put("enable.idempotence", true);

        customConfigs.forEach((k, v) -> {
            producerConfig.put(k, v);
        });
        this.producer = new KafkaProducer<>(producerConfig, ser, ser);
    }

    @Override
    public long append(LogEntry logEntry) throws IOException {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(this.topicName, this.partitionId, null,
                logEntry));
        RecordMetadata recordMetadata;
        try {
            recordMetadata = future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("append kafka failed", e);
            throw new MaxGraphException(e);
        }
        return recordMetadata.offset();
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }
}
