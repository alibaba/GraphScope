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

import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;

public class GraphPropertyMapper {
    private int id;
    private String name;
    private String dataType;
    private String comment;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public static GraphPropertyMapper parseFromGrapyProperty(GraphProperty graphProperty) {
        GraphPropertyMapper propertyMapper = new GraphPropertyMapper();
        propertyMapper.setId(graphProperty.getId());
        propertyMapper.setName(graphProperty.getName());
        propertyMapper.setComment(graphProperty.getComment());
        propertyMapper.setDataType(graphProperty.getDataType().toString());
        return propertyMapper;
    }

    public GraphProperty toGraphProperty() {
        return new DefaultGraphProperty(this.name, this.id, DataType.parseString(this.dataType));
    }
}
