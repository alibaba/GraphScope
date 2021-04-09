package com.alibaba.maxgraph.v2.frontend.compiler.tree.value;

import com.alibaba.maxgraph.proto.v2.VariantType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class ValueValueType implements ValueType {
    private VariantType dataType;

    public ValueValueType(VariantType dataType) {
        this.dataType = dataType;
    }

    public VariantType getDataType() {
        return dataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueValueType that = (ValueValueType) o;
        return dataType == that.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataType);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("dataType", dataType)
                .toString();
    }
}
