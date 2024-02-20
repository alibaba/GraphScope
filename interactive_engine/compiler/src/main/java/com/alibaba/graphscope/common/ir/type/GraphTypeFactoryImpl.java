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
        return super.leastRestrictive(types);
    }
}
