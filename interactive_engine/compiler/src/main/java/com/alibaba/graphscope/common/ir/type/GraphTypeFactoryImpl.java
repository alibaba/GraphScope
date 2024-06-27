/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.type;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.FrontendConfig;
import com.google.common.collect.Lists;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.List;

public class GraphTypeFactoryImpl extends JavaTypeFactoryImpl {
    private final Configs configs;

    public GraphTypeFactoryImpl(Configs configs) {
        super();
        this.configs = configs;
    }

    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        RelDataType newType;
        if (type instanceof GraphSchemaType) {
            GraphSchemaType schemaType = (GraphSchemaType) type;
            if (schemaType.getSchemaTypeAsList().size() > 1) { // fuzzy schema type
                newType =
                        new GraphSchemaType(
                                schemaType.getScanOpt(),
                                schemaType.getLabelType(),
                                schemaType.getFieldList(),
                                schemaType.getSchemaTypeAsList(),
                                nullable);
            } else {
                newType =
                        new GraphSchemaType(
                                schemaType.getScanOpt(),
                                schemaType.getLabelType(),
                                schemaType.getFieldList(),
                                nullable);
            }
        } else if (type instanceof GraphPathType) {
            newType = new GraphPathType(((GraphPathType) type).getComponentType(), nullable);
        } else {
            newType = super.createTypeWithNullability(type, nullable);
        }
        return newType;
    }

    @Override
    public Charset getDefaultCharset() {
        return Charset.forName(FrontendConfig.CALCITE_DEFAULT_CHARSET.get(configs));
    }

    public RelDataType createArbitraryArrayType(
            List<RelDataType> componentTypes, boolean isNullable) {
        return new ArbitraryArrayType(componentTypes, isNullable);
    }

    public RelDataType createArbitraryMapType(
            List<RelDataType> keyTypes, List<RelDataType> valueTypes, boolean isNullable) {
        return new ArbitraryMapType(keyTypes, valueTypes, isNullable);
    }

    @Override
    public @Nullable RelDataType leastRestrictive(List<RelDataType> types) {
        if (types.stream().anyMatch(t -> t instanceof GraphLabelType)) {
            for (RelDataType type : types) {
                if (!(type instanceof GraphLabelType)) return null;
            }
            return types.get(0);
        }
        if (types.stream().anyMatch(t -> t instanceof ArbitraryMapType)) {
            return leastRestrictiveForArbitraryMapType(types);
        }
        return super.leastRestrictive(types);
    }

    // re-implement lease-restrictive type inference for arbitrary map types
    // for each key type and value type, check if they have a least-restrictive type, otherwise
    // return null
    private @Nullable RelDataType leastRestrictiveForArbitraryMapType(List<RelDataType> types) {
        boolean isNullable = false;
        List<List<RelDataType>> leastKeyTypes = Lists.newArrayList();
        List<List<RelDataType>> leastValueTypes = Lists.newArrayList();
        for (RelDataType type : types) {
            if (!(type instanceof ArbitraryMapType)) return null;
            ArbitraryMapType mapType = (ArbitraryMapType) type;
            if (mapType.isNullable()) isNullable = true;
            if (leastKeyTypes.isEmpty() || leastValueTypes.isEmpty()) {
                for (RelDataType keyType : mapType.getKeyTypes()) {
                    leastKeyTypes.add(Lists.newArrayList(keyType));
                }
                for (RelDataType valueType : mapType.getValueTypes()) {
                    leastValueTypes.add(Lists.newArrayList(valueType));
                }
            } else {
                if (leastKeyTypes.size() != mapType.getKeyTypes().size()
                        || leastValueTypes.size() != mapType.getValueTypes().size()) {
                    return null;
                }
                for (int i = 0; i < leastKeyTypes.size(); i++) {
                    leastKeyTypes.get(i).add(mapType.getKeyTypes().get(i));
                }
                for (int i = 0; i < leastValueTypes.size(); i++) {
                    leastValueTypes.get(i).add(mapType.getValueTypes().get(i));
                }
            }
        }
        List<RelDataType> mapKeyTypes = Lists.newArrayList();
        for (List<RelDataType> leastKeyType : leastKeyTypes) {
            RelDataType type = leastRestrictive(leastKeyType);
            if (type == null) return null;
            mapKeyTypes.add(type);
        }
        List<RelDataType> mapValueTypes = Lists.newArrayList();
        for (List<RelDataType> leastValueType : leastValueTypes) {
            RelDataType type = leastRestrictive(leastValueType);
            if (type == null) return null;
            mapValueTypes.add(type);
        }
        return createArbitraryMapType(mapKeyTypes, mapValueTypes, isNullable);
    }
}
