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
package com.alibaba.graphscope.groot.common.schema.impl;

import com.alibaba.graphscope.groot.common.exception.InvalidArgumentException;
import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.api.GraphVertex;
import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.google.common.base.MoreObjects;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DefaultGraphVertex implements GraphVertex {
    private final int labelId;
    private final String label;
    private final List<GraphProperty> propertyList;
    private final List<String> primaryKeyList;

    private final int versionId;

    private final long tableId;

    public DefaultGraphVertex(
            int labelId,
            String label,
            List<GraphProperty> propertyList,
            List<String> primaryKeyList) {
        this(labelId, label, propertyList, primaryKeyList, 0, 0);
    }

    public DefaultGraphVertex(
            int labelId,
            String label,
            List<GraphProperty> propertyList,
            List<String> primaryKeyList,
            int versionId,
            long tableId) {
        this.label = label;
        this.labelId = labelId;
        this.propertyList = propertyList;
        this.primaryKeyList = primaryKeyList;

        this.versionId = versionId;
        this.tableId = tableId;
    }

    public DefaultGraphVertex(TypeDef typeDef, long tableId) {
        this(
                typeDef.getLabelId(),
                typeDef.getLabel(),
                typeDef.getPropertyList(),
                typeDef.getPrimaryKeyNameList(),
                typeDef.getVersionId(),
                tableId);
    }

    @Override
    public List<String> getPrimaryKeyNameList() {
        return primaryKeyList;
    }

    @Override
    public List<GraphProperty> getPrimaryKeyList() {
        List<GraphProperty> props = new ArrayList<>();
        for (String name : primaryKeyList) {
            props.add(getProperty(name));
        }
        return props;
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
        throw new InvalidArgumentException("Can't get graph property for id " + propId);
    }

    @Override
    public GraphProperty getProperty(String propName) {
        for (GraphProperty graphProperty : propertyList) {
            if (Objects.equals(propName, graphProperty.getName())) {
                return graphProperty;
            }
        }
        throw new InvalidArgumentException("Can't get graph property for name " + propName);
    }

    @Override
    public int getVersionId() {
        return versionId;
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
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultGraphVertex that = (DefaultGraphVertex) o;
        return labelId == that.labelId
                && versionId == that.versionId
                && tableId == that.tableId
                && label.equals(that.label)
                && Objects.equals(propertyList, that.propertyList)
                && Objects.equals(primaryKeyList, that.primaryKeyList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labelId, label, propertyList, primaryKeyList, versionId, tableId);
    }
}
