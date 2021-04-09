package com.alibaba.maxgraph.v2.frontend.graph.memory.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.exception.GraphPropertyNotFoundException;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.PrimaryKeyConstraint;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;
import com.google.common.base.MoreObjects;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Default graph vertex in memory for testing
 */
public class DefaultVertexType implements VertexType {
    private String label;
    private int labelId;
    private List<GraphProperty> propertyList;
    private PrimaryKeyConstraint primaryKeyConstraint;
    private int versionId;
    private long tableId;

    public DefaultVertexType(String label,
                             int labelId,
                             List<GraphProperty> propertyList,
                             List<String> primaryKeyList,
                             int versionId,
                             long tableId) {
        this.label = label;
        this.labelId = labelId;
        this.propertyList = propertyList;
        this.primaryKeyConstraint = new PrimaryKeyConstraint(primaryKeyList);
        this.versionId = versionId;
        this.tableId = tableId;
    }

    @Override
    public PrimaryKeyConstraint getPrimaryKeyConstraint() {
        return this.primaryKeyConstraint;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public int getLabelId() {
        return this.labelId;
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

        throw new GraphPropertyNotFoundException("property with id " + propertyId + " not found in vertex " + this.label);
    }

    @Override
    public GraphProperty getProperty(String propertyName) throws GraphPropertyNotFoundException {
        for (GraphProperty property : this.propertyList) {
            if (StringUtils.equals(property.getName(), propertyName)) {
                return property;
            }
        }

        throw new GraphPropertyNotFoundException("property with name " + propertyName + " not found in vertex " + this.label);
    }

    @Override
    public int getVersionId() {
        return this.versionId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("label", this.getLabel())
                .add("labelId", this.getLabelId())
                .add("propertyList", this.getPropertyList())
                .add("primaryKeyConstraint", this.getPrimaryKeyConstraint())
                .add("versionId", this.getVersionId())
                .add("tableId", this.getTableId())
                .toString();
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }
}
