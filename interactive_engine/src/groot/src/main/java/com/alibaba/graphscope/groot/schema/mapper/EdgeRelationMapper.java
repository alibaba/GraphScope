/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.schema.mapper;

import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;

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

    public static EdgeRelationMapper parseFromEdgeRelation(
            String edgeLabel, EdgeRelation edgeRelation) {
        EdgeRelationMapper edgeRelationMapper = new EdgeRelationMapper();
        edgeRelationMapper.setSrcVertexLabel(edgeRelation.getSource().getLabel());
        edgeRelationMapper.setDstVertexLabel(edgeRelation.getTarget().getLabel());
        edgeRelationMapper.setEdgeLabel(edgeLabel);
        edgeRelationMapper.setTableId(edgeRelation.getTableId());
        return edgeRelationMapper;
    }

    public EdgeRelation toEdgeRelation(Map<String, GraphVertex> vertexTypeMap) {
        GraphVertex sourceGraphVertex = vertexTypeMap.get(this.srcVertexLabel);
        GraphVertex targetGraphVertex = vertexTypeMap.get(this.dstVertexLabel);
        return new DefaultEdgeRelation(sourceGraphVertex, targetGraphVertex, this.tableId);
    }
}
