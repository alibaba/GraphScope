package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.DataRecordPb;
import com.alibaba.graphscope.proto.groot.VertexRecordKeyPb;
import com.alibaba.graphscope.proto.groot.WriteRequestPb;
import com.alibaba.graphscope.proto.groot.WriteTypePb;

import java.util.Map;

public class Vertex {
    public String label;
    public Map<String, String> properties;

    public Vertex(String label) {
        this(label, null);
    }

    public Vertex(String label, Map<String, String> properties) {
        this.label = label;
        this.properties = properties;
    }

    public String getLabel() {
        return label;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public VertexRecordKeyPb toVertexRecordKey(String label) {
        VertexRecordKeyPb.Builder builder = VertexRecordKeyPb.newBuilder().setLabel(label);
        //        builder.putAllPkProperties(properties);
        return builder.build();
    }

    public DataRecordPb toDataRecord() {
        DataRecordPb.Builder builder =
                DataRecordPb.newBuilder().setVertexRecordKey(toVertexRecordKey(label));
        if (properties != null) {
            builder.putAllProperties(properties);
        }
        return builder.build();
    }

    public WriteRequestPb toWriteRequest(WriteTypePb writeType) {
        return WriteRequestPb.newBuilder()
                .setWriteType(writeType)
                .setDataRecord(toDataRecord())
                .build();
    }
}
