package com.alibaba.maxgraph.v2.common.schema;

import com.alibaba.maxgraph.v2.common.frontend.api.schema.GraphProperty;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.PrimaryKeyConstraint;
import com.alibaba.maxgraph.v2.common.frontend.api.schema.VertexType;

import java.util.ArrayList;
import java.util.List;

public class VertexTypeImpl implements VertexType {

    private TypeDef typeDef;
    private PrimaryKeyConstraint primaryKeyConstraint;
    private long tableId;

    public VertexTypeImpl(TypeDef typeDef, long tableId) {
        this.typeDef = typeDef;
        List<PropertyDef> properties = typeDef.getProperties();
        List<Integer> pkIdxs = typeDef.getPkIdxs();
        List<String> pkNameList = new ArrayList<>(pkIdxs.size());
        for (Integer pkIdx : pkIdxs) {
            PropertyDef propertyDef = properties.get(pkIdx);
            pkNameList.add(propertyDef.getName());
        }
        this.primaryKeyConstraint = new PrimaryKeyConstraint(pkNameList);
        this.tableId = tableId;
    }

    @Override
    public PrimaryKeyConstraint getPrimaryKeyConstraint() {
        return this.primaryKeyConstraint;
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
        return this.typeDef.getVersionId();
    }

    @Override
    public long getTableId() {
        return this.tableId;
    }
}
