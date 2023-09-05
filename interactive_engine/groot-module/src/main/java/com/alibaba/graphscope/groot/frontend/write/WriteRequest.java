package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.groot.common.util.EdgeRecordKey;
import com.alibaba.graphscope.groot.common.util.VertexRecordKey;
import com.alibaba.graphscope.groot.operation.OperationType;
import com.alibaba.graphscope.proto.groot.DataRecordPb;
import com.alibaba.graphscope.proto.groot.WriteRequestPb;
import com.alibaba.graphscope.proto.groot.WriteTypePb;

import java.util.Collections;
import java.util.Map;

public class WriteRequest {

    private OperationType operationType;
    private DataRecord dataRecord;

    public WriteRequest(OperationType operationType, DataRecord dataRecord) {
        this.operationType = operationType;
        this.dataRecord = dataRecord;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public DataRecord getDataRecord() {
        return dataRecord;
    }

    public static WriteRequest parseProto(WriteRequestPb proto) {
        WriteTypePb writeTypePb = proto.getWriteType();
        DataRecordPb dataRecordPb = proto.getDataRecord();
        Map<String, Object> properties =
                Collections.unmodifiableMap(dataRecordPb.getPropertiesMap());
        DataRecordPb.RecordKeyCase recordKeyCase = dataRecordPb.getRecordKeyCase();
        switch (recordKeyCase) {
            case VERTEX_RECORD_KEY:
                VertexRecordKey vertexRecordKey =
                        VertexRecordKey.parseProto(dataRecordPb.getVertexRecordKey());
                return buildWriteVertexRequest(
                        writeTypePb, new DataRecord(vertexRecordKey, properties));
            case EDGE_RECORD_KEY:
                EdgeRecordKey edgeRecordKey =
                        EdgeRecordKey.parseProto(dataRecordPb.getEdgeRecordKey());
                return buildWriteEdgeRequest(
                        writeTypePb, new DataRecord(edgeRecordKey, properties));
            default:
                throw new IllegalArgumentException(
                        "Invalid record key case [" + recordKeyCase + "]");
        }
    }

    private static WriteRequest buildWriteVertexRequest(WriteTypePb typePb, DataRecord dataRecord) {
        switch (typePb) {
            case INSERT:
                return new WriteRequest(OperationType.OVERWRITE_VERTEX, dataRecord);
            case UPDATE:
                return new WriteRequest(OperationType.UPDATE_VERTEX, dataRecord);
            case DELETE:
                return new WriteRequest(OperationType.DELETE_VERTEX, dataRecord);
            case CLEAR_PROPERTY:
                return new WriteRequest(OperationType.CLEAR_VERTEX_PROPERTIES, dataRecord);
            default:
                throw new IllegalArgumentException("Invalid write type [" + typePb + "]");
        }
    }

    private static WriteRequest buildWriteEdgeRequest(WriteTypePb typePb, DataRecord dataRecord) {
        switch (typePb) {
            case INSERT:
                return new WriteRequest(OperationType.OVERWRITE_EDGE, dataRecord);
            case UPDATE:
                return new WriteRequest(OperationType.UPDATE_EDGE, dataRecord);
            case DELETE:
                return new WriteRequest(OperationType.DELETE_EDGE, dataRecord);
            case CLEAR_PROPERTY:
                return new WriteRequest(OperationType.CLEAR_EDGE_PROPERTIES, dataRecord);
            default:
                throw new IllegalArgumentException("Invalid write type [" + typePb + "]");
        }
    }
}
