package com.alibaba.maxgraph.v2.frontend.compiler.tree.value;

import com.alibaba.maxgraph.proto.v2.VariantType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class PropertyValueType implements ValueType {
    private VariantType propValueType;

    public PropertyValueType(VariantType propValueType) {
        this.propValueType = propValueType;
    }

    public VariantType getPropValueType() {
        return propValueType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertyValueType that = (PropertyValueType) o;
        return propValueType == that.propValueType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(propValueType);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("propValueType", propValueType)
                .toString();
    }
}
