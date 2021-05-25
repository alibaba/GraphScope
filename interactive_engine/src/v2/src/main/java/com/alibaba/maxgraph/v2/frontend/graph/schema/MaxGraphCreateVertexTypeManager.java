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
import com.alibaba.maxgraph.v2.common.frontend.api.manager.CreateVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Create vertex type manager, includes proeprty and primary key related methods
 */
public class MaxGraphCreateVertexTypeManager implements CreateVertexTypeManager {
    private ElementTypePropertyManager elementTypePropertyManager;
    private List<String> primaryKeyList;

    public MaxGraphCreateVertexTypeManager(String vertexLabel) {
        if (null == vertexLabel) {
            throw new GraphCreateSchemaException("vertex label cant be null");
        }

        this.elementTypePropertyManager = new ElementTypePropertyManager(vertexLabel);
    }

    @Override
    public CreateVertexTypeManager primaryKey(String... primaryKeys) {
        if (primaryKeys.length == 0) {
            throw new GraphCreateSchemaException("primary key cant be empty");
        }
        List<String> primaryKeyList = Lists.newArrayList(primaryKeys);
        Set<String> primaryKeySet = Sets.newHashSet(primaryKeyList);
        if (primaryKeyList.size() != primaryKeySet.size()) {
            throw new GraphCreateSchemaException("there's duplicate key in primary keys " + primaryKeyList);
        }
        for (String primaryKey : primaryKeyList) {
            if (!elementTypePropertyManager.checkPropertyExist(primaryKey)) {
                throw new GraphCreateSchemaException("there's no property " + primaryKey + " when use it as primary key");
            }
        }
        this.primaryKeyList = primaryKeyList;

        return this;
    }

    @Override
    public List<String> getPrimaryKeyList() {
        if (null == this.primaryKeyList) {
            throw new GraphCreateSchemaException("primary key cant be null for vertex type " + this.elementTypePropertyManager.getLabel());
        }
        return this.primaryKeyList;
    }

    @Override
    public CreateVertexTypeManager addProperty(String propertyName, String dataType) {
        return addProperty(propertyName, dataType, null);
    }

    @Override
    public CreateVertexTypeManager addProperty(String propertyName, String dataType, String comment) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, comment);
        return this;
    }

    @Override
    public CreateVertexTypeManager addProperty(String propertyName, String dataType, String comment, Object defaultValue) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, comment, defaultValue);
        return this;
    }

    @Override
    public CreateVertexTypeManager comment(String comment) {
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
                .add("primaryKeyList", this.getPrimaryKeyList())
                .toString();
    }
}
