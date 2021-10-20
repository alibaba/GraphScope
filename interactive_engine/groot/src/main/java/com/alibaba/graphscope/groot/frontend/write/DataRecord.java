package com.alibaba.graphscope.groot.frontend.write;

import java.util.Map;

public class DataRecord {

    private VertexRecordKey vertexRecordKey;
    private EdgeRecordKey edgeRecordKey;
    private EdgeTarget edgeTarget;
    private Map<String, Object> properties;

    public DataRecord(VertexRecordKey vertexRecordKey, Map<String, Object> properties) {
        this(vertexRecordKey, null, null, properties);
    }

    public DataRecord(EdgeRecordKey edgeRecordKey, Map<String, Object> properties) {
        this(null, edgeRecordKey, null, properties);
    }

    public DataRecord(EdgeTarget edgeTarget, Map<String, Object> properties) {
        this(null, null, edgeTarget, properties);
    }

    public DataRecord(
            VertexRecordKey vertexRecordKey,
            EdgeRecordKey edgeRecordKey,
            EdgeTarget edgeTarget,
            Map<String, Object> properties) {
        this.vertexRecordKey = vertexRecordKey;
        this.edgeRecordKey = edgeRecordKey;
        this.edgeTarget = edgeTarget;
        this.properties = properties;
    }

    public VertexRecordKey getVertexRecordKey() {
        return vertexRecordKey;
    }

    public EdgeRecordKey getEdgeRecordKey() {
        return edgeRecordKey;
    }

    public EdgeTarget getEdgeTarget() {
        return edgeTarget;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
