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
package com.alibaba.graphscope.groot.schema;

import com.alibaba.maxgraph.compiler.api.schema.DataType;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.proto.groot.PropertyDefPb;

import java.util.Objects;

public class PropertyDef implements GraphProperty {
    private int id;
    private int innerId;
    private String name;
    private DataType dataType;
    private PropertyValue defaultValue;
    private boolean pk;
    private String comment;

    public PropertyDef(int id, int innerId, String name, DataType dataType, PropertyValue defaultValue, boolean pk,
                       String comment) {
        this.id = id;
        this.innerId = innerId;
        this.name = name;
        this.dataType = dataType;
        this.defaultValue = defaultValue;
        this.pk = pk;
        this.comment = comment;
    }

    @Override
    public int getId() {
        return this.id;
    }

    public String getName() {
        return name;
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
        return defaultValue != null;
    }

    @Override
    public Object getDefaultValue() {
        if (defaultValue == null) {
            return null;
        }
        return defaultValue.getValue();
    }

    public PropertyValue getDefaultPropertyValue() {
        return this.defaultValue;
    }

    public boolean isPartOfPrimaryKey() {
        return pk;
    }

    public static PropertyDef parseProto(PropertyDefPb proto) {
        int id = proto.getId();
        int innerId = proto.getInnerId();
        String name = proto.getName();
        DataType dataType = DataType.parseProto(proto.getDataType());
        PropertyValue defaultValue = PropertyValue.parseProto(proto.getDefaultValue());
        boolean pk = proto.getPk();
        String comment = proto.getComment();
        return new PropertyDef(id, innerId, name, dataType, defaultValue, pk, comment);
    }

    public PropertyDefPb toProto() {
        PropertyDefPb.Builder builder = PropertyDefPb.newBuilder();
        if (defaultValue != null) {
            builder.setDefaultValue(defaultValue.toProto());
        }
        builder.setId(id)
                .setInnerId(innerId)
                .setName(name)
                .setDataType(dataType.toProto())
                .setPk(pk);
        if (comment != null) {
            builder.setComment(comment);
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        PropertyDef that = (PropertyDef) o;
        return id == that.id &&
                innerId == that.innerId &&
                pk == that.pk &&
                Objects.equals(name, that.name) &&
                dataType == that.dataType &&
                Objects.equals(defaultValue, that.defaultValue) &&
                Objects.equals(comment, that.comment);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(PropertyDef propertyDef) {
        return new Builder(propertyDef);
    }

    public static class Builder {
        private int id;
        private int innerId;
        private String name;
        private DataType dataType;
        private PropertyValue defaultValue;
        private boolean pk;
        private String comment;

        private Builder(PropertyDef propertyDef) {
            this.id = propertyDef.id;
            this.innerId = propertyDef.innerId;
            this.name = propertyDef.name;
            this.dataType = propertyDef.dataType;
            this.defaultValue = propertyDef.defaultValue;
            this.pk = propertyDef.pk;
            this.comment = propertyDef.comment;
        }

        public Builder() {
            this.id = 0;
            this.innerId = 0;
            this.name = "";
            this.dataType = DataType.UNKNOWN;
            this.defaultValue = null;
            this.pk = false;
            this.comment = "";
        }

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setInnerId(int innerId) {
            this.innerId = innerId;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setDataType(DataType dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder setDefaultValue(PropertyValue defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder setPk(boolean pk) {
            this.pk = pk;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public PropertyDef build() {
            return new PropertyDef(id, innerId, name, dataType, defaultValue, pk, comment);
        }
    }
}
