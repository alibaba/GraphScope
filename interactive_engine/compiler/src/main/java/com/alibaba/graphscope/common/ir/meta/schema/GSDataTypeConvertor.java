/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.meta.schema;

import com.alibaba.graphscope.common.ir.type.ArbitraryArrayType;
import com.alibaba.graphscope.common.ir.type.ArbitraryMapType;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

public interface GSDataTypeConvertor<T> {
    T convert(GSDataTypeDesc from);

    GSDataTypeDesc convert(T from);

    class Factory {
        public static GSDataTypeConvertor create(Class tType, @Nullable Object config) {
            if (tType.equals(DataType.class)) {
                return new GSDataTypeConvertor<DataType>() {
                    @Override
                    public DataType convert(GSDataTypeDesc from) {
                        Map<String, Object> typeMap = from.getYamlDesc();
                        Object value;
                        if ((value = typeMap.get("primitive_type")) != null) {
                            switch (value.toString()) {
                                case "DT_SIGNED_INT32":
                                    return DataType.INT;
                                case "DT_SIGNED_INT64":
                                    return DataType.LONG;
                                case "DT_BOOL":
                                    return DataType.BOOL;
                                case "DT_FLOAT":
                                    return DataType.FLOAT;
                                case "DT_DOUBLE":
                                    return DataType.DOUBLE;
                                default:
                                    throw new UnsupportedOperationException(
                                            "can not convert GSDataTypeDesc ["
                                                    + from
                                                    + "] to DataType");
                            }
                        } else if ((value = typeMap.get("string")) != null) {
                            Map<String, Object> strType = (Map<String, Object>) value;
                            if (strType.containsKey("long_text")) {
                                return DataType.STRING;
                            } else {
                                throw new UnsupportedOperationException(
                                        "can not convert GSDataTypeDesc ["
                                                + from
                                                + "] to DataType");
                            }
                        } else if ((value = typeMap.get("temporal")) != null) {
                            Map<String, Object> temporalType = (Map<String, Object>) value;
                            if (temporalType.containsKey("date32")) {
                                return DataType.DATE;
                            } else if (temporalType.containsKey("time32")) {
                                return DataType.TIME32;
                            } else if (temporalType.containsKey("timestamp")) {
                                return DataType.TIMESTAMP;
                            } else {
                                throw new UnsupportedOperationException(
                                        "can not convert GSDataTypeDesc ["
                                                + from
                                                + "] to DataType");
                            }
                        } else {
                            throw new UnsupportedOperationException(
                                    "can not convert GSDataTypeDesc [" + from + "] to DataType");
                        }
                    }

                    @Override
                    public GSDataTypeDesc convert(DataType from) {
                        throw new UnsupportedOperationException(
                                "convert from DataType to GSDataTypeDesc is unsupported yet");
                    }
                };
            } else if (tType.equals(RelDataType.class)) {
                return new GSDataTypeConvertor<RelDataType>() {
                    @Override
                    public RelDataType convert(GSDataTypeDesc from) {
                        RelDataTypeFactory typeFactory = (RelDataTypeFactory) config;
                        Map<String, Object> typeMap = from.getYamlDesc();
                        Object value;
                        if ((value = typeMap.get("primitive_type")) != null) {
                            switch (value.toString()) {
                                case "DT_ANY":
                                    return typeFactory.createSqlType(SqlTypeName.ANY);
                                case "DT_SIGNED_INT32":
                                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                                case "DT_SIGNED_INT64":
                                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                                case "DT_BOOL":
                                    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                                case "DT_FLOAT":
                                    return typeFactory.createSqlType(SqlTypeName.FLOAT);
                                case "DT_DOUBLE":
                                    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                                default:
                                    throw new UnsupportedOperationException(
                                            "can not convert GSDataTypeDesc ["
                                                    + from
                                                    + "] to RelDataType");
                            }
                        } else if ((value = typeMap.get("string")) != null) {
                            Map<String, Object> strType = (Map<String, Object>) value;
                            if (strType.containsKey("long_text")) {
                                return typeFactory.createSqlType(SqlTypeName.CHAR);
                            } else {
                                throw new UnsupportedOperationException(
                                        "can not convert GSDataTypeDesc ["
                                                + from
                                                + "] to RelDataType");
                            }
                        } else if ((value = typeMap.get("temporal")) != null) {
                            Map<String, Object> temporalType = (Map<String, Object>) value;
                            if (temporalType.containsKey("date32")) {
                                return typeFactory.createSqlType(SqlTypeName.DATE);
                            } else if (temporalType.containsKey("time32")) {
                                return typeFactory.createSqlType(SqlTypeName.TIME);
                            } else if (temporalType.containsKey("timestamp")) {
                                return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                            } else {
                                throw new UnsupportedOperationException(
                                        "can not convert GSDataTypeDesc ["
                                                + from
                                                + "] to RelDataType");
                            }
                        } else if ((value = typeMap.get("array")) != null) {
                            Map<String, Object> arrayType = (Map<String, Object>) value;
                            Map<String, Object> componentType =
                                    (Map<String, Object>) arrayType.get("component_type");
                            Preconditions.checkArgument(
                                    componentType != null,
                                    "field 'component_type' is required in array type");
                            return typeFactory.createArrayType(
                                    convert(new GSDataTypeDesc(componentType)), -1);
                        } else if ((value = typeMap.get("map")) != null) {
                            Map<String, Object> mapType = (Map<String, Object>) value;
                            Map<String, Object> keyType =
                                    (Map<String, Object>) mapType.get("key_type");
                            Preconditions.checkArgument(
                                    keyType != null, "field 'key_type' is required in map type");
                            Map<String, Object> valueType =
                                    (Map<String, Object>) mapType.get("value_type");
                            Preconditions.checkArgument(
                                    valueType != null,
                                    "field 'value_type' is required in map type");
                            return typeFactory.createMapType(
                                    convert(new GSDataTypeDesc(keyType)),
                                    convert(new GSDataTypeDesc(valueType)));
                        } else {
                            throw new UnsupportedOperationException(
                                    "can not convert GSDataTypeDesc [" + from + "] to RelDataType");
                        }
                    }

                    @Override
                    public GSDataTypeDesc convert(RelDataType from) {
                        SqlTypeName typeName = from.getSqlTypeName();
                        Map<String, Object> yamlDesc;
                        switch (typeName) {
                            case INTEGER:
                                yamlDesc = ImmutableMap.of("primitive_type", "DT_SIGNED_INT32");
                                break;
                            case BIGINT:
                                yamlDesc = ImmutableMap.of("primitive_type", "DT_SIGNED_INT64");
                                break;
                            case BOOLEAN:
                                yamlDesc = ImmutableMap.of("primitive_type", "DT_BOOL");
                                break;
                            case FLOAT:
                                yamlDesc = ImmutableMap.of("primitive_type", "DT_FLOAT");
                                break;
                            case DOUBLE:
                                yamlDesc = ImmutableMap.of("primitive_type", "DT_DOUBLE");
                                break;
                            case CHAR:
                                Map longTextMap = Maps.newHashMap();
                                longTextMap.put("long_text", null);
                                yamlDesc = ImmutableMap.of("string", longTextMap);
                                break;
                            case DATE:
                                Map dateMap = Maps.newHashMap();
                                dateMap.put("date32", null);
                                yamlDesc = ImmutableMap.of("temporal", dateMap);
                                break;
                            case TIME:
                                Map timeMap = Maps.newHashMap();
                                timeMap.put("time32", null);
                                yamlDesc = ImmutableMap.of("temporal", timeMap);
                                break;
                            case TIMESTAMP:
                                Map timestampMap = Maps.newHashMap();
                                timestampMap.put("timestamp", null);
                                yamlDesc = ImmutableMap.of("temporal", timestampMap);
                                break;
                            case ARRAY:
                            case MULTISET:
                                Map<String, Object> componentType;
                                if (from instanceof ArbitraryArrayType) {
                                    componentType = ImmutableMap.of("primitive_type", "DT_ANY");
                                } else {
                                    componentType = convert(from.getComponentType()).getYamlDesc();
                                }
                                yamlDesc =
                                        ImmutableMap.of(
                                                "array",
                                                ImmutableMap.of(
                                                        "component_type",
                                                        componentType,
                                                        "max_length",
                                                        Integer.MAX_VALUE));
                                break;
                            case MAP:
                                Map<String, Object> keyType;
                                Map<String, Object> valueType;
                                if (from instanceof ArbitraryMapType) {
                                    keyType = ImmutableMap.of("primitive_type", "DT_ANY");
                                    valueType = ImmutableMap.of("primitive_type", "DT_ANY");
                                } else {
                                    keyType = convert(from.getKeyType()).getYamlDesc();
                                    valueType = convert(from.getValueType()).getYamlDesc();
                                }
                                yamlDesc =
                                        ImmutableMap.of(
                                                "map",
                                                ImmutableMap.of(
                                                        "key_type",
                                                        keyType,
                                                        "value_type",
                                                        valueType));
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "can not convert RelDataType ["
                                                + from
                                                + "] to GSDataTypeDesc");
                        }
                        return new GSDataTypeDesc(yamlDesc);
                    }
                };
            } else {
                throw new UnsupportedOperationException(
                        "unsupported type class "
                                + tType
                                + " in GSDataTypeConvertor.Factory.create");
            }
        }
    }
}
