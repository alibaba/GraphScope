package com.alibaba.graphscope.groot.sdk.schema;

import com.alibaba.graphscope.proto.groot.*;

import java.util.Map;

public class Edge {
    public String label;
    public String srcLabel;
    public String dstLabel;
    public Map<String, String> srcPk;
    public Map<String, String> dstPk;
    public Map<String, String> properties;

    public long eid;

    /**
     * Construct an edge
     * @param label edge label
     * @param srcLabel source vertex label
     * @param dstLabel destination vertex label
     * @param srcPk source primary keys
     * @param dstPk destination primary keys
     * @param properties edge properties
     */
    public Edge(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk,
            Map<String, String> properties) {
        this.label = label;
        this.srcLabel = srcLabel;
        this.dstLabel = dstLabel;
        this.srcPk = srcPk;
        this.dstPk = dstPk;
        this.properties = properties;
    }

    public Edge(
            String label,
            String srcLabel,
            String dstLabel,
            Map<String, String> srcPk,
            Map<String, String> dstPk) {
        this(label, srcLabel, dstLabel, srcPk, dstPk, null);
    }

    public Edge(String label, Vertex src, Vertex dst, Map<String, String> properties) {
        this(
                label,
                src.getLabel(),
                dst.getLabel(),
                src.getProperties(),
                dst.getProperties(),
                properties);
    }

    public Edge(String label, Vertex src, Vertex dst) {
        this(label, src, dst, null);
    }

    public void setEid(long eid) {
        this.eid = eid;
    }

    public String getLabel() {
        return label;
    }

    public String getSrcLabel() {
        return srcLabel;
    }

    public String getDstLabel() {
        return dstLabel;
    }

    public Map<String, String> getSrcPk() {
        return srcPk;
    }

    public Map<String, String> getDstPk() {
        return dstPk;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public EdgeRecordKeyPb toEdgeRecordKey() {
        return EdgeRecordKeyPb.newBuilder()
                .setLabel(label)
                .setSrcVertexKey(toVertexRecordKey(srcLabel, srcPk))
                .setDstVertexKey(toVertexRecordKey(dstLabel, dstPk))
                .setInnerId(eid)
                .build();
    }

    public DataRecordPb toDataRecord() {
        DataRecordPb.Builder builder =
                DataRecordPb.newBuilder().setEdgeRecordKey(toEdgeRecordKey());
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

    private VertexRecordKeyPb toVertexRecordKey(String label, Map<String, String> properties) {
        VertexRecordKeyPb.Builder builder = VertexRecordKeyPb.newBuilder().setLabel(label);
        builder.putAllPkProperties(properties);
        return builder.build();
    }
}
