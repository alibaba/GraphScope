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

import com.alibaba.maxgraph.compiler.api.schema.EdgeRelation;
import com.alibaba.maxgraph.compiler.api.schema.GraphEdge;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.PrimaryKeyConstraint;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GraphEdgeImpl implements GraphEdge {

    private TypeDef typeDef;
    private List<EdgeRelation> edgeRelations;
    private PrimaryKeyConstraint primaryKeyConstraint;

    public GraphEdgeImpl(TypeDef typeDef, List<EdgeRelation> edgeRelations) {
        this.typeDef = typeDef;
        this.edgeRelations = edgeRelations;
        List<Integer> pkIdxs = typeDef.getPkIdxs();
        if (pkIdxs != null && pkIdxs.size() > 0) {
            List<PropertyDef> properties = typeDef.getProperties();
            List<String> pkNameList = new ArrayList<>(pkIdxs.size());
            for (Integer pkIdx : pkIdxs) {
                PropertyDef propertyDef = properties.get(pkIdx);
                pkNameList.add(propertyDef.getName());
            }
            this.primaryKeyConstraint = new PrimaryKeyConstraint(pkNameList);
        }
    }

    @Override
    public List<EdgeRelation> getRelationList() {
        return edgeRelations;
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
        return typeDef.getVersionId();
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GraphEdgeImpl graphEdge = (GraphEdgeImpl) o;

        return Objects.equals(typeDef, graphEdge.typeDef)
                && Objects.equals(edgeRelations, graphEdge.edgeRelations)
                && Objects.equals(primaryKeyConstraint, graphEdge.primaryKeyConstraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeDef, edgeRelations, primaryKeyConstraint);
    }
}
