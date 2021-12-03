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
package com.alibaba.graphscope.groot.schema;

import com.alibaba.maxgraph.compiler.api.schema.GraphElement;
import com.alibaba.maxgraph.compiler.api.schema.GraphProperty;
import com.alibaba.maxgraph.compiler.api.schema.PrimaryKeyConstraint;
import com.alibaba.maxgraph.proto.groot.PropertyDefPb;
import com.alibaba.maxgraph.proto.groot.TypeDefPb;
import com.alibaba.graphscope.groot.operation.LabelId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TypeDef implements GraphElement {
    private int versionId;

    private String label;
    private LabelId labelId;

    private List<PropertyDef> properties;
    private List<Integer> pkIdxs;
    private Map<String, Integer> nameToIdx;
    private Map<Integer, Integer> idToIdx;
    private TypeEnum typeEnum;

    private TypeDef(
            TypeEnum typeEnum,
            int versionId,
            String label,
            LabelId labelId,
            List<PropertyDef> properties) {
        this.typeEnum = typeEnum;
        this.versionId = versionId;
        this.label = label;
        this.labelId = labelId;
        this.properties = new ArrayList<>(properties);
        this.pkIdxs = new ArrayList<>();
        this.nameToIdx = new HashMap<>();
        this.idToIdx = new HashMap<>();
        for (int i = 0; i < properties.size(); i++) {
            PropertyDef propertyDef = this.properties.get(i);
            if (propertyDef.isPartOfPrimaryKey()) {
                this.pkIdxs.add(i);
            }
            this.nameToIdx.put(propertyDef.getName(), i);
            this.idToIdx.put(propertyDef.getId(), i);
        }
    }

    public List<Integer> getPkIdxs() {
        return Collections.unmodifiableList(pkIdxs);
    }

    @Override
    public int getVersionId() {
        return versionId;
    }

    @Override
    public PrimaryKeyConstraint getPrimaryKeyConstraint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Integer> getPkPropertyIndices() {
        return getPkIdxs();
    }

    @Override
    public List<GraphProperty> getPrimaryKeyList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public int getLabelId() {
        return labelId.getId();
    }

    public LabelId getTypeLabelId() {
        return labelId;
    }

    @Override
    public List<GraphProperty> getPropertyList() {
        return new ArrayList<>(getProperties());
    }

    @Override
    public GraphProperty getProperty(int propId) {
        Integer idx = idToIdx.get(propId);
        if (idx == null) {
            return null;
        }
        return properties.get(idx);
    }

    @Override
    public GraphProperty getProperty(String propName) {
        Integer idx = nameToIdx.get(propName);
        if (idx == null) {
            return null;
        }
        return properties.get(idx);
    }

    public List<PropertyDef> getProperties() {
        return Collections.unmodifiableList(properties);
    }

    public TypeEnum getTypeEnum() {
        return typeEnum;
    }

    public static TypeDef parseProto(TypeDefPb proto) {
        int versionId = proto.getVersionId();
        String label = proto.getLabel();
        LabelId labelId = LabelId.parseProto(proto.getLabelId());
        List<PropertyDef> propertyDefs = new ArrayList<>();
        List<PropertyDefPb> propsList = proto.getPropsList();
        for (PropertyDefPb propertyDefPb : propsList) {
            propertyDefs.add(PropertyDef.parseProto(propertyDefPb));
        }
        TypeEnum typeEnum = TypeEnum.parseProto(proto.getTypeEnum());
        return new TypeDef(typeEnum, versionId, label, labelId, propertyDefs);
    }

    public TypeDefPb toProto() {
        TypeDefPb.Builder builder = TypeDefPb.newBuilder();
        builder.setVersionId(versionId);
        builder.setLabel(label);
        builder.setLabelId(labelId.toProto());
        for (PropertyDef property : properties) {
            builder.addProps(property.toProto());
        }
        builder.setTypeEnum(typeEnum.toProto());
        return builder.build();
    }

    public TypeDefPb toDdlProto() {
        TypeDefPb.Builder builder = TypeDefPb.newBuilder();
        builder.setLabel(label);
        for (PropertyDef property : properties) {
            builder.addProps(property.toProto());
        }
        if (typeEnum != null) {
            builder.setTypeEnum(typeEnum.toProto());
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TypeDef typeDef = (TypeDef) o;
        return versionId == typeDef.versionId
                && Objects.equals(label, typeDef.label)
                && Objects.equals(labelId, typeDef.labelId)
                && Objects.equals(properties, typeDef.properties);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(TypeDef typeDef) {
        return new Builder(typeDef);
    }

    public static class Builder {
        private int versionId;
        private String label;
        private LabelId labelId;
        private List<PropertyDef> properties = new ArrayList<>();
        private TypeEnum typeEnum;

        private Builder() {}

        private Builder(TypeDef typeDef) {
            this.versionId = typeDef.getVersionId();
            this.label = typeDef.getLabel();
            this.labelId = typeDef.getTypeLabelId();
            this.properties = typeDef.getProperties();
            this.typeEnum = typeDef.typeEnum;
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setLabelId(LabelId labelId) {
            this.labelId = labelId;
            return this;
        }

        public Builder addPropertyDef(PropertyDef propertyDef) {
            this.properties.add(propertyDef);
            return this;
        }

        public Builder setPropertyDefs(List<PropertyDef> propertyDefs) {
            this.properties = propertyDefs;
            return this;
        }

        public Builder setTypeEnum(TypeEnum typeEnum) {
            this.typeEnum = typeEnum;
            return this;
        }

        public TypeDef build() {
            return new TypeDef(typeEnum, versionId, label, labelId, properties);
        }
    }
}
