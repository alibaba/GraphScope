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
