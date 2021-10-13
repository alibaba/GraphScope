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
package com.alibaba.maxgraph.compiler.schema;

import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.GraphVertex;
import com.alibaba.maxgraph.compiler.api.schema.PrimaryKeyConstraint;
import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class DefaultGraphVertex implements GraphVertex {
    private int labelId;
    private String label;
    private List<GraphProperty> propertyList;
    private List<GraphProperty> primaryKeyList;

    public DefaultGraphVertex(int id, String label, List<GraphProperty> propertyList, List<GraphProperty> primaryKeyList) {
        this.labelId = id;
        this.label = label;
        this.propertyList = propertyList;
        this.primaryKeyList = primaryKeyList;
    }

    @Override
    public List<GraphProperty> getPrimaryKeyList() {
        return primaryKeyList;
    }

    @Override
    public List<Integer> getPkPropertyIndices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrimaryKeyConstraint getPrimaryKeyConstraint() {
        return null;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public int getLabelId() {
        return labelId;
    }

    @Override
    public List<GraphProperty> getPropertyList() {
        return propertyList;
    }

    @Override
    public GraphProperty getProperty(int propId) {
        for (GraphProperty graphProperty : propertyList) {
            if (graphProperty.getId() == propId) {
                return graphProperty;
            }
        }
        throw new IllegalArgumentException("Cant get graph property for id " + propId);
    }

    @Override
    public GraphProperty getProperty(String propName) {
        for (GraphProperty graphProperty : propertyList) {
            if (StringUtils.equals(propName, graphProperty.getName())) {
                return graphProperty;
            }
        }
        throw new IllegalArgumentException("Cant get graph property for name " + propName);
    }

    @Override
    public int getVersionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("labelId", labelId)
                .add("label", label)
                .add("propertyList", propertyList)
                .add("primaryKeyList", primaryKeyList)
                .toString();
    }

    @Override
    public long getTableId() {
        throw new UnsupportedOperationException();
    }
}
