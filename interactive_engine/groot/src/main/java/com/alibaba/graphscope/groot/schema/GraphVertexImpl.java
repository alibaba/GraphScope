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
package com.alibaba.graphscope.groot.schema;

import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.compiler.api.schema.PrimaryKeyConstraint;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GraphVertexImpl implements GraphVertex {

    private TypeDef typeDef;
    private PrimaryKeyConstraint primaryKeyConstraint;
    private long tableId;

    public GraphVertexImpl(TypeDef typeDef, long tableId) {
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
    public List<GraphProperty> getPrimaryKeyList() {
        return null;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphVertexImpl that = (GraphVertexImpl) o;
        return tableId == that.tableId
                && Objects.equals(typeDef, that.typeDef)
                && Objects.equals(primaryKeyConstraint, that.primaryKeyConstraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeDef, primaryKeyConstraint, tableId);
    }
}
