package com.alibaba.maxgraph.v2.common.wal.kafka;

import com.alibaba.maxgraph.v2.common.wal.LogEntry;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class LogEntrySerializer implements Serializer<LogEntry> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, LogEntry data) {
        if (data == null) {
            return null;
        }
        return data.toProto().toByteArray();
    }

    @Override
    public void close() {

    }
}
