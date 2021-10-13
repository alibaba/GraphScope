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

import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.PrimaryKeyConstraint;
import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class DefaultGraphEdge implements GraphEdge {
    private int id;
    private String label;
    private List<GraphProperty> propertyList;
    private List<EdgeRelation> relationList;

    public DefaultGraphEdge(int id, String label, List<GraphProperty> propertyList, List<EdgeRelation> relationList) {
        this.id = id;
        this.label = label;
        this.propertyList = propertyList;
        this.relationList = relationList;
    }

    @Override
    public List<EdgeRelation> getRelationList() {
        return relationList;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public int getLabelId() {
        return id;
    }

    @Override
    public List<GraphProperty> getPropertyList() {
        return propertyList;
    }

    @Override
    public GraphProperty getProperty(int propId) {
        for (GraphProperty property : propertyList) {
            if (property.getId() == propId) {
                return property;
            }
        }

        throw new IllegalArgumentException("Invalid property id " + propId);
    }

    @Override
    public GraphProperty getProperty(String propName) {
        for (GraphProperty property : propertyList) {
            if (StringUtils.equals(propName, property.getName())) {
                return property;
            }
        }

        throw new IllegalArgumentException("Invalid property name " + propName);
    }

    @Override
    public int getVersionId() {
        return 0;
    }

    @Override
    public List<GraphProperty> getPrimaryKeyList() {
        return null;
    }

    @Override
    public List<Integer> getPkPropertyIndices() {
        return null;
    }

    @Override
    public PrimaryKeyConstraint getPrimaryKeyConstraint() {
        return null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("label", label)
                .add("propertyList", propertyList)
                .add("relationList", relationList)
                .toString();
    }
}
