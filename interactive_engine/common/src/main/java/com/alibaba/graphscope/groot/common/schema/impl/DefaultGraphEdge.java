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
import com.alibaba.graphscope.groot.common.schema.api.EdgeRelation;
import com.alibaba.graphscope.groot.common.schema.api.GraphEdge;
import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.wrapper.TypeDef;
import com.google.common.base.MoreObjects;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DefaultGraphEdge implements GraphEdge {
    private final int id;
    private final String label;
    private final List<GraphProperty> propertyList;
    private final List<EdgeRelation> relationList;
    private List<String> primaryKeyList;

    private final int versionId;

    public DefaultGraphEdge(
            int id,
            String label,
            List<GraphProperty> propertyList,
            List<EdgeRelation> relationList) {
        this(id, label, propertyList, relationList, 0);
    }

    public DefaultGraphEdge(
            int id,
            String label,
            List<GraphProperty> propertyList,
            List<EdgeRelation> relationList,
            List<String> primaryKeyList) {
        this(id, label, propertyList, relationList, primaryKeyList, 0);
    }

    public DefaultGraphEdge(
            int id,
            String label,
            List<GraphProperty> propertyList,
            List<EdgeRelation> relationList,
            int versionId) {
        this.id = id;
        this.label = label;
        this.propertyList = propertyList;
        this.relationList = relationList;
        this.versionId = versionId;
    }

    public DefaultGraphEdge(
            int id,
            String label,
            List<GraphProperty> propertyList,
            List<EdgeRelation> relationList,
            List<String> primaryKeyList,
            int versionId) {
        this.id = id;
        this.label = label;
        this.propertyList = propertyList;
        this.relationList = relationList;
        this.primaryKeyList = primaryKeyList;
        this.versionId = versionId;
    }

    public DefaultGraphEdge(TypeDef typeDef, List<EdgeRelation> edgeRelations) {
        this(
                typeDef.getLabelId(),
                typeDef.getLabel(),
                typeDef.getPropertyList(),
                edgeRelations,
                typeDef.getPrimaryKeyNameList(),
                typeDef.getVersionId());
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

        throw new InvalidArgumentException("Invalid property id " + propId);
    }

    @Override
    public GraphProperty getProperty(String propName) {
        for (GraphProperty property : propertyList) {
            if (Objects.equals(propName, property.getName())) {
                return property;
            }
        }

        throw new InvalidArgumentException("Invalid property name " + propName);
    }

    @Override
    public int getVersionId() {
        return this.versionId;
    }

    @Override
    public List<GraphProperty> getPrimaryKeyList() {
        List<GraphProperty> props = new ArrayList<>();
        if (this.primaryKeyList != null) {
            for (String name : primaryKeyList) {
                props.add(getProperty(name));
            }
        }
        return props;
    }

    @Override
    public List<String> getPrimaryKeyNameList() {
        return primaryKeyList;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("label", label)
                .add("propertyList", propertyList)
                .add("relationList", relationList)
                .add("versionId", versionId)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultGraphEdge that = (DefaultGraphEdge) o;
        return id == that.id
                && versionId == that.versionId
                && label.equals(that.label)
                && Objects.equals(propertyList, that.propertyList)
                && Objects.equals(relationList, that.relationList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, label, propertyList, relationList, versionId);
    }
}
