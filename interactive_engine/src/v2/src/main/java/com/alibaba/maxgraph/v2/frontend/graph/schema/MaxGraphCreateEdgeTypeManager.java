package com.alibaba.maxgraph.v2.frontend.graph.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.CreateEdgeTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.EdgeRelationEntity;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Create edge type, includes property and relation related methods
 */
public class MaxGraphCreateEdgeTypeManager implements CreateEdgeTypeManager {
    private ElementTypePropertyManager elementTypePropertyManager;
    private List<EdgeRelationEntity> relationLabelList = Lists.newArrayList();

    public MaxGraphCreateEdgeTypeManager(String label) {
        this.elementTypePropertyManager = new ElementTypePropertyManager(label);
    }

    @Override
    public CreateEdgeTypeManager addRelation(String sourceVertexLabel, String targetVertexLabel) {
        EdgeRelationEntity relationPair = new EdgeRelationEntity(sourceVertexLabel, targetVertexLabel);
        if (relationLabelList.contains(relationPair)) {
            throw new GraphCreateSchemaException("relation ship <" + sourceVertexLabel + "->" + elementTypePropertyManager.getLabel() + "->" + targetVertexLabel + "> exist already");
        }
        relationLabelList.add(relationPair);

        return this;
    }

    @Override
    public List<EdgeRelationEntity> getRelationList() {
        return this.relationLabelList;
    }

    @Override
    public CreateEdgeTypeManager addProperty(String propertyName, String dataType) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, null);
        return this;
    }

    @Override
    public CreateEdgeTypeManager addProperty(String propertyName, String dataType, String comment) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, comment);
        return this;
    }

    @Override
    public CreateEdgeTypeManager addProperty(String propertyName, String dataType, String comment, Object defaultValue) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, comment, defaultValue);
        return this;
    }

    @Override
    public CreateEdgeTypeManager comment(String comment) {
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
                .add("relationLabelList", getRelationList())
                .toString();
    }
}
