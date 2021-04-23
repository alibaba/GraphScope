package com.alibaba.maxgraph.v2.frontend.compiler.tree.value;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class MapEntryValueType implements ValueType {
    private ValueType keyType;
    private ValueType valueType;

    public MapEntryValueType(ValueType keyType, ValueType valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MapEntryValueType that = (MapEntryValueType) o;
        return Objects.equal(keyType, that.keyType) &&
                Objects.equal(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(keyType, valueType);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("keyType", keyType)
                .add("valueType", valueType)
                .toString();
    }

    public ValueType getKey() {
        return keyType;
    }

    public ValueType getValue() {
        return valueType;
    }
}
