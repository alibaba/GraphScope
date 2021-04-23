package com.alibaba.maxgraph.v2.frontend.compiler.tree.value;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;

public class PathValueType implements ValueType {
    private ValueType pathValueType;

    public PathValueType() {
        this.pathValueType = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PathValueType that = (PathValueType) o;
        return Objects.equal(pathValueType, that.pathValueType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pathValueType);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("pathValueType", pathValueType)
                .toString();
    }

    public void addPathValueType(ValueType valueType) {
        if (this.pathValueType == null) {
            this.pathValueType = valueType;
        } else if (!pathValueType.equals(valueType)) {
            this.pathValueType = new VarietyValueType(Sets.newHashSet(pathValueType, valueType));
        }
    }

    public ValueType getPathValue() {
        return pathValueType;
    }
}
