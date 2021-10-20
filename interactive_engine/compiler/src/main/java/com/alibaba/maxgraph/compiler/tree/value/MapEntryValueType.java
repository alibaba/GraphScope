/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.compiler.tree.value;

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
