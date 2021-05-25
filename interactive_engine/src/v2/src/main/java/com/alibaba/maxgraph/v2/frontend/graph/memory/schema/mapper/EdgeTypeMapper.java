/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.v2.frontend.graph.memory.schema.mapper;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeRelation;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.EdgeType;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.alibaba.maxgraph.v2.common.schema.TypeEnum;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultEdgeType;
import com.alibaba.maxgraph.v2.frontend.graph.memory.schema.DefaultVertexType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EdgeTypeMapper extends SchemaElementMapper {
    @JSONField
    private List<EdgeRelationMapper> relationShips;

    public static SchemaElementMapper parseFromEdgeType(EdgeType edgeType) {
        EdgeTypeMapper edgeTypeMapper = new EdgeTypeMapper();
        edgeTypeMapper.setId(edgeType.getLabelId());
        edgeTypeMapper.setLabel(edgeType.getLabel());
        edgeTypeMapper.setType(TypeEnum.EDGE.toString());

        List<EdgeRelationMapper> relationMapperList = Lists.newArrayList();
        for (EdgeRelation edgeRelation : edgeType.getRelationList()) {
            relationMapperList.add(EdgeRelationMapper.parseFromEdgeRelation(edgeType.getLabel(), edgeRelation));
        }
        edgeTypeMapper.setRelationShips(relationMapperList);

        List<GraphPropertyMapper> propertyMapperList = Lists.newArrayList();
        for (GraphProperty graphProperty : edgeType.getPropertyList()) {
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

    public EdgeType toEdgeType(Map<String, VertexType> vertexTypeMap) {
        List<GraphProperty> graphPropertyList = null == this.getPropertyDefList() ? Lists.newArrayList() :
                this.getPropertyDefList().stream().map(GraphPropertyMapper::toGraphProperty).collect(Collectors.toList());
        List<EdgeRelation> relationList = Lists.newArrayList();
        if (null != this.relationShips) {
            for (EdgeRelationMapper relationMapper : this.relationShips) {
                relationList.add(relationMapper.toEdgeRelation(vertexTypeMap));
            }
        }
        return new DefaultEdgeType(this.getLabel(), this.getId(), graphPropertyList, relationList, this.getVersionId());
    }
}
