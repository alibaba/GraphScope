/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.groot.schema.mapper;

import com.alibaba.maxgraph.compiler.api.exception.GraphSchemaException;
import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.google.common.base.MoreObjects;

/** Default graph property in memory for testing */
public class DefaultGraphProperty implements GraphProperty {
    private String name;
    private int id;
    private DataType dataType;
    private String comment;
    private boolean hasDefaultValue;
    private Object defaultValue;

    public DefaultGraphProperty(String name, int id, DataType dataType) {
        this(name, id, dataType, "", false, null);
    }

    public DefaultGraphProperty(
            String name,
            int id,
            DataType dataType,
            String comment,
            boolean hasDefaultValue,
            Object defaultValue) {
        if ((hasDefaultValue && defaultValue == null)
                || (!hasDefaultValue && defaultValue != null)) {
            throw new GraphSchemaException(
                    "invalid property for hasDefaultValue["
                            + hasDefaultValue
                            + "] and defaultValue["
                            + defaultValue
                            + "]");
        }
        this.name = name;
        this.id = id;
        this.dataType = dataType;
        this.comment = comment;
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;
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
        return this.comment;
    }

    @Override
    public boolean hasDefaultValue() {
        return this.hasDefaultValue;
    }

    @Override
    public Object getDefaultValue() {
        return this.defaultValue;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", this.getName())
                .add("id", this.getId())
                .add("dataType", this.getDataType())
                .add("comment", this.getComment())
                .add("hasDefaultValue", this.hasDefaultValue())
                .add("defaultValue", this.getDefaultValue())
                .toString();
    }
}
