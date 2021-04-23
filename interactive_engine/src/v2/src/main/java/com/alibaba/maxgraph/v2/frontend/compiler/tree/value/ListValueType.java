package com.alibaba.maxgraph.v2.frontend.compiler.tree.value;

import com.google.common.base.MoreObjects;

/**
 * The list value type
 */
public class ListValueType implements ValueType {
    private ValueType listValue;

    public ListValueType(ValueType listValue) {
        this.listValue = listValue;
    }

    public ValueType getListValue() {
        return listValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListValueType that = (ListValueType) o;
        return com.google.common.base.Objects.equal(listValue, that.listValue);
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(listValue);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("listValue", listValue)
                .toString();
    }
}
