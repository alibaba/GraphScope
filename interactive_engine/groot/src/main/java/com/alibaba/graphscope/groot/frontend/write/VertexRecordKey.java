package com.alibaba.graphscope.groot.frontend.write;

import com.alibaba.graphscope.proto.write.VertexRecordKeyPb;

import java.util.Collections;
import java.util.Map;

public class VertexRecordKey {
    private String label;
    private Map<String, Object> properties;

    public VertexRecordKey(String label) {
        this(label, Collections.EMPTY_MAP);
    }

    public VertexRecordKey(String label, Map<String, Object> properties) {
        this.label = label;
        this.properties = properties;
    }

    public String getLabel() {
        return label;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public static VertexRecordKey parseProto(VertexRecordKeyPb proto) {
        String label = proto.getLabel();
        Map<String, Object> pkPropertiesMap =
                Collections.unmodifiableMap(proto.getPkPropertiesMap());
        return new VertexRecordKey(label, pkPropertiesMap);
    }

    public VertexRecordKeyPb toProto() {
        VertexRecordKeyPb.Builder builder = VertexRecordKeyPb.newBuilder();
        builder.setLabel(label);
        properties.forEach((k, v) -> builder.putPkProperties(k, v.toString()));
        return builder.build();
    }
}
