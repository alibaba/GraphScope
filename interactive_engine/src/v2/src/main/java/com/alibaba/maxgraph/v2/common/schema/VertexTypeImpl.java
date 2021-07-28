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
package com.alibaba.maxgraph.v2.common.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.PrimaryKeyConstraint;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;

import java.util.ArrayList;
import java.util.List;

public class VertexTypeImpl implements VertexType {

    private TypeDef typeDef;
    private PrimaryKeyConstraint primaryKeyConstraint;
    private long tableId;

    public VertexTypeImpl(TypeDef typeDef, long tableId) {
        this.typeDef = typeDef;
        List<PropertyDef> properties = typeDef.getProperties();
        List<Integer> pkIdxs = typeDef.getPkIdxs();
        List<String> pkNameList = new ArrayList<>(pkIdxs.size());
        for (Integer pkIdx : pkIdxs) {
            PropertyDef propertyDef = properties.get(pkIdx);
            pkNameList.add(propertyDef.getName());
        }
        this.primaryKeyConstraint = new PrimaryKeyConstraint(pkNameList);
        this.tableId = tableId;
    }

    @Override
    public PrimaryKeyConstraint getPrimaryKeyConstraint() {
        return this.primaryKeyConstraint;
    }

    @Override
    public List<Integer> getPkPropertyIndices() {
        return this.typeDef.getPkPropertyIndices();
    }

    @Override
    public String getLabel() {
        return typeDef.getLabel();
    }

    @Override
    public int getLabelId() {
        return typeDef.getLabelId();
    }

    @Override
    public List<GraphProperty> getPropertyList() {
        return typeDef.getPropertyList();
    }

    @Override
    public GraphProperty getProperty(int propId) {
        return typeDef.getProperty(propId);
    }

    @Override
    public GraphProperty getProperty(String propName) {
        return typeDef.getProperty(propName);
    }

    @Override
    public int getVersionId() {
        return this.typeDef.getVersionId();
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }
}
