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
package com.alibaba.graphscope.groot.common.schema.mapper;

import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.common.schema.api.EdgeRelation;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphEdge;
import com.alibaba.graphscope.groot.common.schema.wrapper.TypeEnum;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EdgeTypeMapper extends SchemaElementMapper {
    private List<EdgeRelationMapper> relationShips;

    private List<ElementIndexMapper> indexes;

    public static SchemaElementMapper parseFromEdgeType(GraphEdge graphEdge) {
        EdgeTypeMapper edgeTypeMapper = new EdgeTypeMapper();
        edgeTypeMapper.setVersionId(graphEdge.getVersionId());
        edgeTypeMapper.setId(graphEdge.getLabelId());
        edgeTypeMapper.setLabel(graphEdge.getLabel());
        edgeTypeMapper.setType(TypeEnum.EDGE.toString());
        ElementIndexMapper elementIndexMapper = new ElementIndexMapper();
        elementIndexMapper.setName("primary_key");
        elementIndexMapper.setIndexType("PRIMARY_KEY");
        elementIndexMapper.setPropertyNames(graphEdge.getPrimaryKeyNameList());
        ArrayList<ElementIndexMapper> elementIndexMapperList = new ArrayList<>();
        elementIndexMapperList.add(elementIndexMapper);
        edgeTypeMapper.setIndexes(elementIndexMapperList);
        List<EdgeRelationMapper> relationMapperList = new ArrayList<>();
        for (EdgeRelation edgeRelation : graphEdge.getRelationList()) {
            relationMapperList.add(
                    EdgeRelationMapper.parseFromEdgeRelation(graphEdge.getLabel(), edgeRelation));
        }
        edgeTypeMapper.setRelationShips(relationMapperList);

        List<GraphPropertyMapper> propertyMapperList = new ArrayList<>();
        for (GraphProperty graphProperty : graphEdge.getPropertyList()) {
            propertyMapperList.add(GraphPropertyMapper.parseFromGraphProperty(graphProperty));
        }
        edgeTypeMapper.setPropertyDefList(propertyMapperList);

        return edgeTypeMapper;
    }

    public List<EdgeRelationMapper> getRelationShips() {
        return relationShips;
    }

    public List<ElementIndexMapper> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<ElementIndexMapper> indexes) {
        this.indexes = indexes;
    }

    public void setRelationShips(List<EdgeRelationMapper> relationShips) {
        this.relationShips = relationShips;
    }

    public GraphEdge toEdgeType(Map<String, GraphVertex> vertexTypeMap) {
        List<GraphProperty> graphPropertyList =
                null == this.getPropertyDefList()
                        ? new ArrayList<>()
                        : this.getPropertyDefList().stream()
                                .map(GraphPropertyMapper::toGraphProperty)
                                .collect(Collectors.toList());
        List<EdgeRelation> relationList = new ArrayList<>();
        if (null != this.relationShips) {
            for (EdgeRelationMapper relationMapper : this.relationShips) {
                relationList.add(relationMapper.toEdgeRelation(vertexTypeMap));
            }
        }
        List<String> primaryKeyList = new ArrayList<>();
        if (indexes != null && indexes.size() > 0) {
            if (indexes.size() > 1) {
                throw new InvalidArgumentException(
                        "Only support primary key now for " + this.indexes);
            }
            primaryKeyList = indexes.get(0).getPropertyNames();
        }

        return new DefaultGraphEdge(
                this.getId(),
                this.getLabel(),
                graphPropertyList,
                relationList,
                primaryKeyList,
                this.getVersionId());
    }
}
