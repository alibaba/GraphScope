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
import com.google.common.collect.Maps;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            Map<RexNode, ArbitraryMapType.KeyValueType> keyValueTypeMap, boolean isNullable) {
        return new ArbitraryMapType(keyValueTypeMap, isNullable);
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
        Map<RexNode, List<ArbitraryMapType.KeyValueType>> leastKeyValueTypes = Maps.newHashMap();
        for (RelDataType type : types) {
            if (!(type instanceof ArbitraryMapType)) return null;
            ArbitraryMapType mapType = (ArbitraryMapType) type;
            if (mapType.isNullable()) isNullable = true;
            if (leastKeyValueTypes.isEmpty()) {
                mapType.getKeyValueTypeMap()
                        .forEach(
                                (k, v) -> {
                                    leastKeyValueTypes.put(k, Lists.newArrayList(v));
                                });
            } else {
                for (Map.Entry<RexNode, ArbitraryMapType.KeyValueType> entry :
                        mapType.getKeyValueTypeMap().entrySet()) {
                    List<ArbitraryMapType.KeyValueType> leastTypes =
                            leastKeyValueTypes.get(entry.getKey());
                    if (leastTypes == null) {
                        return null;
                    }
                    leastTypes.add(entry.getValue());
                }
            }
        }
        Map<RexNode, ArbitraryMapType.KeyValueType> leastKeyValueType = Maps.newHashMap();
        for (Map.Entry<RexNode, List<ArbitraryMapType.KeyValueType>> entry :
                leastKeyValueTypes.entrySet()) {
            RelDataType leastKeyType =
                    leastRestrictive(
                            entry.getValue().stream()
                                    .map(ArbitraryMapType.KeyValueType::getKey)
                                    .collect(Collectors.toList()));
            if (leastKeyType == null) return null;
            RelDataType leastValueType =
                    leastRestrictive(
                            entry.getValue().stream()
                                    .map(ArbitraryMapType.KeyValueType::getValue)
                                    .collect(Collectors.toList()));
            if (leastValueType == null) return null;
            leastKeyValueType.put(
                    entry.getKey(),
                    new ArbitraryMapType.KeyValueType(leastKeyType, leastValueType));
        }
        return createArbitraryMapType(leastKeyValueType, isNullable);
    }
}
