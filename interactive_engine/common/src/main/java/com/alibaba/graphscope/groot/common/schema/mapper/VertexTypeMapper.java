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
import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.alibaba.graphscope.groot.common.schema.impl.DefaultGraphVertex;
import com.alibaba.graphscope.groot.common.schema.wrapper.TypeEnum;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class VertexTypeMapper extends SchemaElementMapper {
    private List<ElementIndexMapper> indexes;
    private long tableId;

    public static VertexTypeMapper parseFromVertexType(GraphVertex graphVertex) {
        VertexTypeMapper vertexTypeMapper = new VertexTypeMapper();
        vertexTypeMapper.setId(graphVertex.getLabelId());
        vertexTypeMapper.setLabel(graphVertex.getLabel());
        vertexTypeMapper.setType(TypeEnum.VERTEX.toString());

        ElementIndexMapper elementIndexMapper = new ElementIndexMapper();
        elementIndexMapper.setName("primary_key");
        elementIndexMapper.setIndexType("PRIMARY_KEY");
        elementIndexMapper.setPropertyNames(graphVertex.getPrimaryKeyNameList());
        ArrayList<ElementIndexMapper> elementIndexMapperList = new ArrayList<>();
        elementIndexMapperList.add(elementIndexMapper);
        vertexTypeMapper.setIndexes(elementIndexMapperList);
        List<GraphPropertyMapper> propertyMapperList = new ArrayList<>();
        for (GraphProperty graphProperty : graphVertex.getPropertyList()) {
            propertyMapperList.add(GraphPropertyMapper.parseFromGraphProperty(graphProperty));
        }
        vertexTypeMapper.setPropertyDefList(propertyMapperList);
        vertexTypeMapper.setVersionId(graphVertex.getVersionId());
        vertexTypeMapper.setTableId(graphVertex.getTableId());
        return vertexTypeMapper;
    }

    public List<ElementIndexMapper> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<ElementIndexMapper> indexes) {
        this.indexes = indexes;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public GraphVertex toVertexType() {
        List<GraphProperty> graphPropertyList =
                this.getPropertyDefList().stream()
                        .map(GraphPropertyMapper::toGraphProperty)
                        .collect(Collectors.toList());
        /// TODO only support primary key now
        if (this.indexes.size() > 1) {
            throw new InvalidArgumentException("Only support primary key now for " + this.indexes);
        }
        List<String> primaryKeyList = indexes.get(0).getPropertyNames();

        return new DefaultGraphVertex(
                this.getId(),
                this.getLabel(),
                graphPropertyList,
                primaryKeyList,
                this.getVersionId(),
                this.getTableId());
    }
}
