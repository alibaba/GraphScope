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
package com.alibaba.graphscope.sdkcommon.schema.mapper;

import com.alibaba.graphscope.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.graphscope.compiler.api.schema.GraphProperty;
import com.alibaba.graphscope.compiler.api.schema.GraphVertex;
import com.alibaba.graphscope.compiler.api.schema.PrimaryKeyConstraint;

import java.util.List;

/** Default graph vertex in memory for testing */
public class DefaultGraphVertex implements GraphVertex {
    private final String label;
    private final int labelId;
    private final List<GraphProperty> propertyList;
    private final PrimaryKeyConstraint primaryKeyConstraint;
    private final int versionId;
    private final long tableId;

    public DefaultGraphVertex(
            String label,
            int labelId,
            List<GraphProperty> propertyList,
            List<String> primaryKeyList,
            int versionId,
            long tableId) {
        this.label = label;
        this.labelId = labelId;
        this.propertyList = propertyList;
        this.primaryKeyConstraint = new PrimaryKeyConstraint(primaryKeyList);
        this.versionId = versionId;
        this.tableId = tableId;
    }

    @Override
    public PrimaryKeyConstraint getPrimaryKeyConstraint() {
        return this.primaryKeyConstraint;
    }

    @Override
    public List<Integer> getPkPropertyIndices() {
        return null;
    }

    @Override
    public List<GraphProperty> getPrimaryKeyList() {
        return null;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public int getLabelId() {
        return this.labelId;
    }

    @Override
    public List<GraphProperty> getPropertyList() {
        return this.propertyList;
    }

    @Override
    public GraphProperty getProperty(int propertyId) throws GraphPropertyNotFoundException {
        for (GraphProperty property : this.propertyList) {
            if (property.getId() == propertyId) {
                return property;
            }
        }

        throw new GraphPropertyNotFoundException(
                "property with id " + propertyId + " not found in vertex " + this.label);
    }

    @Override
    public GraphProperty getProperty(String propertyName) throws GraphPropertyNotFoundException {
        for (GraphProperty property : this.propertyList) {
            if (property.getName().equals(propertyName)) {
                return property;
            }
        }

        throw new GraphPropertyNotFoundException(
                "property with name " + propertyName + " not found in vertex " + this.label);
    }

    @Override
    public int getVersionId() {
        return this.versionId;
    }

    @Override
    public String toString() {
        return "DefaultGraphVertex{"
                + "label='"
                + label
                + '\''
                + ", labelId="
                + labelId
                + ", propertyList="
                + propertyList
                + ", primaryKeyConstraint="
                + primaryKeyConstraint
                + ", versionId="
                + versionId
                + ", tableId="
                + tableId
                + '}';
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }
}
