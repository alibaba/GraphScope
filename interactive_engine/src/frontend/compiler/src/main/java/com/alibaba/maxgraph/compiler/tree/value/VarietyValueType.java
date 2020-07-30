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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The value type will be one in the list
 */
public class VarietyValueType implements ValueType {
    private Set<ValueType> valueTypeList;

    public VarietyValueType(Collection<ValueType> valueTypeList) {
        this.valueTypeList = Sets.newHashSet();
        for (ValueType valueType : valueTypeList) {
            if (valueType instanceof VarietyValueType) {
                this.valueTypeList.addAll(VarietyValueType.class.cast(valueType).getValueTypeList());
            } else {
                this.valueTypeList.add(valueType);
            }
        }
        checkArgument(valueTypeList.size() > 1, "Only one value type in valueTypeList");
    }

    public Set<ValueType> getValueTypeList() {
        return valueTypeList;
    }

    public boolean hasVertexValueType() {
        for (ValueType valueType : valueTypeList) {
            if (valueType instanceof VertexValueType) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode() {
        return valueTypeList.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (null == that || getClass() != that.getClass()) {
            return false;
        }

        VarietyValueType varietyValueType = VarietyValueType.class.cast(that);
        return valueTypeList.equals(varietyValueType.valueTypeList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("option", valueTypeList)
                .toString();
    }

    public ValueType getListValueType() {
        List<ValueType> listValueTypeSet = Lists.newArrayList();
        for (ValueType valueType : valueTypeList) {
            if (valueType instanceof ListValueType) {
                listValueTypeSet.add(valueType);
            }
        }
        if (listValueTypeSet.isEmpty()) {
            throw new IllegalArgumentException("list value type empty in variety value type=>" + this.toString());
        }
        if (listValueTypeSet.size() == 1) {
            return listValueTypeSet.get(0);
        } else {
            return new VarietyValueType(listValueTypeSet);
        }
    }

    public ValueType getOtherValueType() {
        List<ValueType> otherValueTypeSet = Lists.newArrayList();
        for (ValueType valueType : valueTypeList) {
            if (!(valueType instanceof ListValueType)) {
                otherValueTypeSet.add(valueType);
            }
        }
        if (otherValueTypeSet.isEmpty()) {
            throw new IllegalArgumentException("other value type empty in variety value type=>" + this.toString());
        }
        if (otherValueTypeSet.size() == 1) {
            return otherValueTypeSet.get(0);
        } else {
            return new VarietyValueType(otherValueTypeSet);
        }

    }

    public MapValueType getMapValueType() {
        List<MapValueType> listValueTypeSet = Lists.newArrayList();
        for (ValueType valueType : valueTypeList) {
            if (valueType instanceof MapValueType) {
                listValueTypeSet.add((MapValueType) valueType);
            }
        }
        if (listValueTypeSet.isEmpty()) {
            throw new IllegalArgumentException("map value type empty in variety value type=>" + this.toString());
        }
        if (listValueTypeSet.size() == 1) {
            return listValueTypeSet.get(0);
        } else {
            throw new IllegalArgumentException("There's multiple map value in variety value type=>" + this.toString());
        }
    }
}
