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

import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.compiler.api.schema.PrimaryKeyConstraint;
import com.alibaba.graphscope.groot.schema.TypeEnum;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

public class VertexTypeMapper extends SchemaElementMapper {
    private List<VertexIndexMapper> indexes;
    private long tableId;

    public static VertexTypeMapper parseFromVertexType(GraphVertex graphVertex) {
        VertexTypeMapper vertexTypeMapper = new VertexTypeMapper();
        vertexTypeMapper.setId(graphVertex.getLabelId());
        vertexTypeMapper.setLabel(graphVertex.getLabel());
        vertexTypeMapper.setType(TypeEnum.VERTEX.toString());

        PrimaryKeyConstraint primaryKeyConstraint = graphVertex.getPrimaryKeyConstraint();
        VertexIndexMapper vertexIndexMapper = new VertexIndexMapper();
        vertexIndexMapper.setName("primary_key");
        vertexIndexMapper.setIndexType("PRIMARY_KEY");
        vertexIndexMapper.setPropertyNames(primaryKeyConstraint.getPrimaryKeyList());
        vertexTypeMapper.setIndexes(Lists.newArrayList(vertexIndexMapper));

        List<GraphPropertyMapper> propertyMapperList = Lists.newArrayList();
        for (GraphProperty graphProperty : graphVertex.getPropertyList()) {
            propertyMapperList.add(GraphPropertyMapper.parseFromGrapyProperty(graphProperty));
        }
        vertexTypeMapper.setPropertyDefList(propertyMapperList);
        vertexTypeMapper.setVersionId(graphVertex.getVersionId());
        vertexTypeMapper.setTableId(graphVertex.getTableId());
        return vertexTypeMapper;
    }

    public List<VertexIndexMapper> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<VertexIndexMapper> indexes) {
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
        List<String> primaryKeyList = Lists.newArrayList();
        /// TODO only support primary key now
        if (this.indexes.size() == 1) {
            primaryKeyList.addAll(indexes.get(0).getPropertyNames());
        } else if (this.indexes.size() > 1) {
            throw new IllegalArgumentException("Only support primary key now for " + this.indexes);
        }
        return new DefaultGraphVertex(
                this.getLabel(),
                this.getId(),
                graphPropertyList,
                primaryKeyList,
                this.getVersionId(),
                this.getTableId());
    }
}
