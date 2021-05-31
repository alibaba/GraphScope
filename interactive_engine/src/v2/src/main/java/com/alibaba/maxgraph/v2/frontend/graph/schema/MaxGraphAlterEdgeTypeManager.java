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
package com.alibaba.maxgraph.v2.frontend.graph.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.AlterEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.EdgeRelationEntity;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.List;

public class MaxGraphAlterEdgeTypeManager implements AlterEdgeTypeManager {
    private ElementTypePropertyManager elementTypePropertyManager;
    private List<String> dropPropertyNames = Lists.newArrayList();
    private List<EdgeRelationEntity> dropRelationList = Lists.newArrayList();
    private List<EdgeRelationEntity> addRelationList = Lists.newArrayList();

    public MaxGraphAlterEdgeTypeManager(String label) {
        this.elementTypePropertyManager = new ElementTypePropertyManager(label);
    }

    @Override
    public AlterEdgeTypeManager addRelation(String sourceVertexLabel, String targetVertexLabel) {
        EdgeRelationEntity relationPair = new EdgeRelationEntity(sourceVertexLabel, targetVertexLabel);
        if (addRelationList.contains(relationPair)) {
            throw new GraphCreateSchemaException("add relation ship <" + sourceVertexLabel + "->" + elementTypePropertyManager.getLabel() + "->" + targetVertexLabel + "> exist already");
        }
        addRelationList.add(relationPair);

        return this;
    }

    @Override
    public AlterEdgeTypeManager dropRelation(String sourceVertexLabel, String targetVertexLabel) {
        EdgeRelationEntity relationPair = new EdgeRelationEntity(sourceVertexLabel, targetVertexLabel);
        if (dropRelationList.contains(relationPair)) {
            throw new GraphCreateSchemaException("drop relation ship <" + sourceVertexLabel + "->" + elementTypePropertyManager.getLabel() + "->" + targetVertexLabel + "> exist already");
        }
        dropRelationList.add(relationPair);

        return this;
    }

    @Override
    public List<EdgeRelationEntity> getAddRelationList() {
        return this.addRelationList;
    }

    @Override
    public List<EdgeRelationEntity> getDropRelationList() {
        return this.dropRelationList;
    }

    @Override
    public AlterEdgeTypeManager dropProperty(String propertyName) {
        if (dropPropertyNames.contains(propertyName)) {
            throw new GraphCreateSchemaException("cant remove one property " + propertyName + " from type "
                    + this.elementTypePropertyManager.getLabel() + " more than one times");
        }
        dropPropertyNames.add(propertyName);

        return this;
    }

    @Override
    public List<String> getDropPropertyNames() {
        return this.dropPropertyNames;
    }


    @Override
    public AlterEdgeTypeManager addProperty(String propertyName, String dataType) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, null);
        return this;
    }

    @Override
    public AlterEdgeTypeManager addProperty(String propertyName, String dataType, String comment) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, comment);
        return this;
    }

    @Override
    public AlterEdgeTypeManager addProperty(String propertyName, String dataType, String comment, Object defaultValue) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, comment, defaultValue);
        return this;
    }

    @Override
    public AlterEdgeTypeManager comment(String comment) {
        this.elementTypePropertyManager.setComment(comment);
        return this;
    }

    @Override
    public String getLabel() {
        return this.elementTypePropertyManager.getLabel();
    }

    @Override
    public List<GraphProperty> getPropertyDefinitions() {
        return this.elementTypePropertyManager.getPropertyDefinitions();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("elementTypePropertyManager", elementTypePropertyManager)
                .add("dropPropertyNames", this.getDropPropertyNames())
                .add("dropRelationList", this.getDropRelationList())
                .add("addRelationList", this.getAddRelationList())
                .toString();
    }
}
