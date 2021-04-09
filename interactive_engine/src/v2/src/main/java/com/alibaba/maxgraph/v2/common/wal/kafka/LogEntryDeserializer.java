package com.alibaba.maxgraph.v2.common.wal.kafka;

import com.alibaba.maxgraph.proto.v2.LogEntryPb;
import com.alibaba.maxgraph.v2.common.exception.MaxGraphException;
import com.alibaba.maxgraph.v2.common.wal.LogEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class LogEntryDeserializer implements Deserializer<LogEntry> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public LogEntry deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return LogEntry.parseProto(LogEntryPb.parseFrom(data));
        } catch (InvalidProtocolBufferException e) {
            throw new MaxGraphException(e);
        }
    }

    @Override
    public void close() {

    }
}
