package com.alibaba.maxgraph.v2.frontend.graph.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphCreateSchemaException;
import com.alibaba.maxgraph.v2.common.frontend.api.manager.AlterVertexTypeManager;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Alter vertex type manager, includes dropProperty/addProeprty methods
 */
public class MaxGraphAlterVertexTypeManager implements AlterVertexTypeManager {
    private ElementTypePropertyManager elementTypePropertyManager;
    private List<String> dropPropertyNames = Lists.newArrayList();

    public MaxGraphAlterVertexTypeManager(String label) {
        this.elementTypePropertyManager = new ElementTypePropertyManager(label);
    }

    @Override
    public AlterVertexTypeManager dropProperty(String propertyName) {
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
    public AlterVertexTypeManager addProperty(String propertyName, String dataType) {
        return this.addProperty(propertyName, dataType, null);
    }

    @Override
    public AlterVertexTypeManager addProperty(String propertyName, String dataType, String comment) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, comment);
        return this;
    }

    @Override
    public AlterVertexTypeManager addProperty(String propertyName, String dataType, String comment, Object defaultValue) {
        this.elementTypePropertyManager.addProperty(propertyName, dataType, comment, defaultValue);
        return this;
    }

    @Override
    public AlterVertexTypeManager comment(String comment) {
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
                .toString();
    }
}
