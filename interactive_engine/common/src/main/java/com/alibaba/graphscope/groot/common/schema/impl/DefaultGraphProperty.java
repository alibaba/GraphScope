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
package com.alibaba.graphscope.groot.common.schema.impl;

import com.alibaba.graphscope.groot.common.schema.api.GraphProperty;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class DefaultGraphProperty implements GraphProperty {
    private final int id;
    private final String name;
    private final DataType dataType;

    private final Object defaultValue;

    public DefaultGraphProperty(int id, String name, DataType dataType) {
        this.id = id;
        this.name = name;
        this.dataType = dataType;
        this.defaultValue = null;
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public DataType getDataType() {
        return this.dataType;
    }

    @Override
    public String getComment() {
        return null;
    }

    @Override
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("name", name)
                .add("dataType", dataType)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultGraphProperty that = (DefaultGraphProperty) o;
        return id == that.id
                && Objects.equal(name, that.name)
                && dataType == that.dataType
                && Objects.equal(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, name, dataType, defaultValue);
    }
}
