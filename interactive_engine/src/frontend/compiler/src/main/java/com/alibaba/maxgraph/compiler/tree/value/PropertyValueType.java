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

import com.alibaba.maxgraph.Message;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class PropertyValueType implements ValueType {
    private Message.VariantType propValueType;

    public PropertyValueType(Message.VariantType propValueType) {
        this.propValueType = propValueType;
    }

    public Message.VariantType getPropValueType() {
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
