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

import com.alibaba.maxgraph.compiler.api.exception.GraphPropertyNotFoundException;
import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.PrimaryKeyConstraint;
import com.google.common.base.MoreObjects;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/** Default graph edge in memory for testing */
public class DefaultGraphEdge implements GraphEdge {
    private String label;
    private int id;
    private List<GraphProperty> propertyList;
    private List<EdgeRelation> relationList;
    private int versionId;

    public DefaultGraphEdge(
            String label,
            int id,
            List<GraphProperty> propertyList,
            List<EdgeRelation> relationList,
            int versionId) {
        this.label = label;
        this.id = id;
        this.propertyList = propertyList;
        this.relationList = relationList;
        this.versionId = versionId;
    }

    @Override
    public List<EdgeRelation> getRelationList() {
        return this.relationList;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public int getLabelId() {
        return this.id;
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
            if (StringUtils.equals(property.getName(), propertyName)) {
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
    public PrimaryKeyConstraint getPrimaryKeyConstraint() {
        return null;
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
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("label", this.getLabel())
                .add("id", this.getLabelId())
                .add("propertyList", this.getPropertyList())
                .add("relationList", this.getRelationList())
                .toString();
    }
}
