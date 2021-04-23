package com.alibaba.maxgraph.v2.frontend.graph.memory.schema.mapper;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultEdgeRelation;

import java.util.Map;

public class EdgeRelationMapper {
    private String srcVertexLabel;
    private String dstVertexLabel;
    private String edgeLabel;
    private long tableId;

    public String getSrcVertexLabel() {
        return srcVertexLabel;
    }

    public void setSrcVertexLabel(String srcVertexLabel) {
        this.srcVertexLabel = srcVertexLabel;
    }

    public String getDstVertexLabel() {
        return dstVertexLabel;
    }

    public void setDstVertexLabel(String dstVertexLabel) {
        this.dstVertexLabel = dstVertexLabel;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public void setEdgeLabel(String edgeLabel) {
        this.edgeLabel = edgeLabel;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public static EdgeRelationMapper parseFromEdgeRelation(String edgeLabel, EdgeRelation edgeRelation) {
        EdgeRelationMapper edgeRelationMapper = new EdgeRelationMapper();
        edgeRelationMapper.setSrcVertexLabel(edgeRelation.getSource().getLabel());
        edgeRelationMapper.setDstVertexLabel(edgeRelation.getTarget().getLabel());
        edgeRelationMapper.setEdgeLabel(edgeLabel);
        edgeRelationMapper.setTableId(edgeRelation.getTableId());
        return edgeRelationMapper;
    }

    public EdgeRelation toEdgeRelation(Map<String, VertexType> vertexTypeMap) {
        VertexType sourceVertexType = vertexTypeMap.get(this.srcVertexLabel);
        VertexType targetVertexType = vertexTypeMap.get(this.dstVertexLabel);
        return new DefaultEdgeRelation(sourceVertexType, targetVertexType, this.tableId);
    }
}
