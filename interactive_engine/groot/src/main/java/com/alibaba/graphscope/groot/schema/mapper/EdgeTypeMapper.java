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

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.graphscope.groot.schema.TypeEnum;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EdgeTypeMapper extends SchemaElementMapper {
    @JSONField private List<EdgeRelationMapper> relationShips;

    public static SchemaElementMapper parseFromEdgeType(GraphEdge graphEdge) {
        EdgeTypeMapper edgeTypeMapper = new EdgeTypeMapper();
        edgeTypeMapper.setId(graphEdge.getLabelId());
        edgeTypeMapper.setLabel(graphEdge.getLabel());
        edgeTypeMapper.setType(TypeEnum.EDGE.toString());

        List<EdgeRelationMapper> relationMapperList = Lists.newArrayList();
        for (EdgeRelation edgeRelation : graphEdge.getRelationList()) {
            relationMapperList.add(
                    EdgeRelationMapper.parseFromEdgeRelation(graphEdge.getLabel(), edgeRelation));
        }
        edgeTypeMapper.setRelationShips(relationMapperList);

        List<GraphPropertyMapper> propertyMapperList = Lists.newArrayList();
        for (GraphProperty graphProperty : graphEdge.getPropertyList()) {
            propertyMapperList.add(GraphPropertyMapper.parseFromGrapyProperty(graphProperty));
        }
        edgeTypeMapper.setPropertyDefList(propertyMapperList);

        return edgeTypeMapper;
    }

    public List<EdgeRelationMapper> getRelationShips() {
        return relationShips;
    }

    public void setRelationShips(List<EdgeRelationMapper> relationShips) {
        this.relationShips = relationShips;
    }

    public GraphEdge toEdgeType(Map<String, GraphVertex> vertexTypeMap) {
        List<GraphProperty> graphPropertyList =
                null == this.getPropertyDefList()
                        ? Lists.newArrayList()
                        : this.getPropertyDefList().stream()
                                .map(GraphPropertyMapper::toGraphProperty)
                                .collect(Collectors.toList());
        List<EdgeRelation> relationList = Lists.newArrayList();
        if (null != this.relationShips) {
            for (EdgeRelationMapper relationMapper : this.relationShips) {
                relationList.add(relationMapper.toEdgeRelation(vertexTypeMap));
            }
        }
        return new DefaultGraphEdge(
                this.getLabel(),
                this.getId(),
                graphPropertyList,
                relationList,
                this.getVersionId());
    }
}
