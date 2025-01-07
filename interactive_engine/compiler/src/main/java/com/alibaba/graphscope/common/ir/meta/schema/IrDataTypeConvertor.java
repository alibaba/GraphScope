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

import static java.util.Objects.requireNonNull;

import com.alibaba.graphscope.common.ir.type.ArbitraryArrayType;
import com.alibaba.graphscope.common.ir.type.ArbitraryMapType;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.groot.common.schema.wrapper.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

/**
 * Define the mutual type conversion between other type systems and Ir type system {@code RelDataType}
 */
public interface IrDataTypeConvertor<T> {
    Logger logger = LoggerFactory.getLogger(IrDataTypeConvertor.class);

    RelDataType convert(T dataFrom);

    T convert(RelDataType dataFrom);

    class Groot implements IrDataTypeConvertor<DataType> {
        private final RelDataTypeFactory typeFactory;

        public Groot(RelDataTypeFactory typeFactory) {
            this.typeFactory = typeFactory;
        }

        @Override
        public RelDataType convert(DataType from) {
            requireNonNull(typeFactory, "typeFactory should not be null");
            switch (from) {
                case BOOL:
                    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                case CHAR:
                    // single character
                    return typeFactory.createSqlType(SqlTypeName.CHAR, 1);
                case STRING:
                    // string with unlimited length
                    return typeFactory.createSqlType(
                            SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED);
                case SHORT:
                    // 2-bytes signed integer
                    return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                case INT:
                    // 4-bytes signed integer
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case LONG:
                    // 8-bytes signed integer
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case FLOAT:
                    // single precision floating point, 4 bytes
                    return typeFactory.createSqlType(SqlTypeName.FLOAT);
                case DOUBLE:
                    // double precision floating point, 8 bytes
                    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                case DATE:
                    // int32 days since 1970-01-01
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                case TIME32:
                    // int32 milliseconds past midnight
                    return typeFactory.createSqlType(SqlTypeName.TIME);
                case TIMESTAMP:
                    // int64 milliseconds since 1970-01-01 00:00:00.000000
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case INT_LIST:
                    // array of 4-bytes signed integer, unlimited size
                    return typeFactory.createArrayType(
                            convert(DataType.INT), RelDataType.PRECISION_NOT_SPECIFIED);
                case LONG_LIST:
                    // array of 8-bytes signed integer, unlimited size
                    return typeFactory.createArrayType(
                            convert(DataType.LONG), RelDataType.PRECISION_NOT_SPECIFIED);
                case FLOAT_LIST:
                    // array of single precision floating point, unlimited size
                    return typeFactory.createArrayType(
                            convert(DataType.FLOAT), RelDataType.PRECISION_NOT_SPECIFIED);
                case DOUBLE_LIST:
                    // array of double precision floating point, unlimited size
                    return typeFactory.createArrayType(
                            convert(DataType.DOUBLE), RelDataType.PRECISION_NOT_SPECIFIED);
                case STRING_LIST:
                    // array of string, unlimited size
                    return typeFactory.createArrayType(
                            convert(DataType.STRING), RelDataType.PRECISION_NOT_SPECIFIED);
                case UNKNOWN:
                    return typeFactory.createSqlType(SqlTypeName.UNKNOWN);
                case BYTES:
                case BYTES_LIST:
                default:
                    throw new UnsupportedOperationException(
                            "convert GrootDataType ["
                                    + from.name()
                                    + "] to RelDataType is unsupported yet");
            }
        }

        @Override
        public DataType convert(RelDataType dataFrom) {
            SqlTypeName typeName = dataFrom.getSqlTypeName();
            switch (typeName) {
                case BOOLEAN:
                    return DataType.BOOL;
                case CHAR:
                    if (dataFrom.getPrecision() == 1) {
                        return DataType.CHAR;
                    }
                case VARCHAR:
                    if (dataFrom.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
                        return DataType.STRING;
                    }
                case SMALLINT:
                    return DataType.SHORT;
                case INTEGER:
                    return DataType.INT;
                case BIGINT:
                    return DataType.LONG;
                case FLOAT:
                    return DataType.FLOAT;
                case DOUBLE:
                    return DataType.DOUBLE;
                case DATE:
                    return DataType.DATE;
                case TIME:
                    return DataType.TIME32;
                case TIMESTAMP:
                    return DataType.TIMESTAMP;
                case MULTISET:
                case ARRAY:
                    RelDataType componentType = dataFrom.getComponentType();
                    // check the array or set is a unlimited size list of primitive type
                    if (componentType != null
                            && dataFrom.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
                        switch (componentType.getSqlTypeName()) {
                            case INTEGER:
                                return DataType.INT_LIST;
                            case BIGINT:
                                return DataType.LONG_LIST;
                            case FLOAT:
                                return DataType.FLOAT_LIST;
                            case DOUBLE:
                                return DataType.DOUBLE_LIST;
                            case VARCHAR:
                                if (componentType.getPrecision()
                                        == RelDataType.PRECISION_NOT_SPECIFIED) {
                                    return DataType.STRING_LIST;
                                }
                        }
                    }
                case UNKNOWN:
                default:
                    return DataType.UNKNOWN;
            }
        }
    }

    class Flex implements IrDataTypeConvertor<GSDataTypeDesc> {
        private final RelDataTypeFactory typeFactory;

        public Flex(RelDataTypeFactory typeFactory) {
            this.typeFactory = typeFactory;
        }

        @Override
        public RelDataType convert(GSDataTypeDesc from) {
            Objects.requireNonNull(typeFactory, "typeFactory should not be null");
            Map<String, Object> typeMap = from.getYamlDesc();
            Object value;
            if ((value = typeMap.get("primitive_type")) != null) {
                switch (value.toString()) {
                    case "DT_NULL":
                        return typeFactory.createSqlType(SqlTypeName.NULL);
                    case "DT_ANY":
                        // any type
                        return typeFactory.createSqlType(SqlTypeName.ANY);
                    case "DT_SIGNED_INT32":
                        // 4-bytes signed integer
                        return typeFactory.createSqlType(SqlTypeName.INTEGER);
                    case "DT_SIGNED_INT64":
                        // 8-bytes signed integer
                        return typeFactory.createSqlType(SqlTypeName.BIGINT);
                    case "DT_BOOL":
                        // boolean type
                        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                    case "DT_FLOAT":
                        // single precision floating point, 4 bytes
                        return typeFactory.createSqlType(SqlTypeName.FLOAT);
                    case "DT_DOUBLE":
                        // double precision floating point, 8 bytes
                        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                }
            } else if ((value = typeMap.get("string")) != null) {
                Map<String, Object> strType = (Map<String, Object>) value;
                if (strType.containsKey("long_text")) {
                    // string with unlimited length
                    return typeFactory.createSqlType(
                            SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED);
                } else if (strType.containsKey("char")) {
                    Object charValue = strType.get("char");
                    Integer fixedLen = getIntValue(charValue, "fixed_length");
                    if (fixedLen == null) {
                        fixedLen =
                                typeFactory.getTypeSystem().getDefaultPrecision(SqlTypeName.CHAR);
                        logger.warn(
                                "can not convert {} to a valid fixed length, use default"
                                        + " length {} instead",
                                charValue,
                                fixedLen);
                    }
                    // string with fixed length
                    return typeFactory.createSqlType(SqlTypeName.CHAR, fixedLen);
                } else if (strType.containsKey("var_char")) {
                    Object varCharValue = strType.get("var_char");
                    Integer maxLen = getIntValue(varCharValue, "max_length");
                    if (maxLen == null) {
                        maxLen =
                                typeFactory
                                        .getTypeSystem()
                                        .getDefaultPrecision(SqlTypeName.VARCHAR);
                        logger.warn(
                                "can not convert {} to a valid max length, use default"
                                        + " length {} instead",
                                varCharValue,
                                maxLen);
                    }
                    // string with variable length, bound by max length
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, maxLen);
                }
            } else if ((value = typeMap.get("temporal")) != null) {
                Map<String, Object> temporalType = (Map<String, Object>) value;
                if (temporalType.containsKey("date32")) {
                    // int32 days since 1970-01-01
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                } else if (temporalType.containsKey("time32")) {
                    // int32 milliseconds past midnight
                    return typeFactory.createSqlType(SqlTypeName.TIME);
                } else if (temporalType.containsKey("timestamp")) {
                    // int64 milliseconds since 1970-01-01 00:00:00.000000
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                }
            } else if ((value = typeMap.get("array")) != null) {
                Map<String, Object> arrayType = (Map<String, Object>) value;
                Map<String, Object> componentType =
                        (Map<String, Object>) arrayType.get("component_type");
                Preconditions.checkArgument(
                        componentType != null, "field 'component_type' is required in array type");
                // array of component type, unlimited size
                return typeFactory.createArrayType(
                        convert(new GSDataTypeDesc(componentType)),
                        RelDataType.PRECISION_NOT_SPECIFIED);
            } else if ((value = typeMap.get("map")) != null) {
                Map<String, Object> mapType = (Map<String, Object>) value;
                Map<String, Object> keyType = (Map<String, Object>) mapType.get("key_type");
                Preconditions.checkArgument(
                        keyType != null, "field 'key_type' is required in map type");
                Map<String, Object> valueType = (Map<String, Object>) mapType.get("value_type");
                Preconditions.checkArgument(
                        valueType != null, "field 'value_type' is required in map type");
                // map of key type to value type
                return typeFactory.createMapType(
                        convert(new GSDataTypeDesc(keyType)),
                        convert(new GSDataTypeDesc(valueType)));
            } else if ((value = typeMap.get("decimal")) != null) {
                Integer precision = getIntValue(value, "precision");
                if (precision == null) {
                    precision =
                            typeFactory.getTypeSystem().getDefaultPrecision(SqlTypeName.DECIMAL);
                    logger.warn(
                            "can not convert {} to a valid precision, use default"
                                    + " precision {} instead",
                            value,
                            precision);
                }
                Integer scale = getIntValue(value, "scale");
                if (scale == null) {
                    scale = typeFactory.getTypeSystem().getMaxScale(SqlTypeName.DECIMAL);
                    logger.warn(
                            "can not convert {} to a valid scale, use max" + " scale {} instead",
                            value,
                            scale);
                }
                // decimal type with precision and scale
                return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
            }
            throw new UnsupportedOperationException(
                    "convert GSDataTypeDesc [" + from + "] to RelDataType is unsupported yet");
        }

        @Override
        public GSDataTypeDesc convert(RelDataType from) {
            if (from instanceof IntervalSqlType) {
                return new GSDataTypeDesc(ImmutableMap.of("primitive_type", "DT_SIGNED_INT64"));
            } else if (from instanceof GraphLabelType) {
                return new GSDataTypeDesc(ImmutableMap.of("primitive_type", "DT_SIGNED_INT32"));
            }
            SqlTypeName typeName = from.getSqlTypeName();
            Map<String, Object> yamlDesc;
            switch (typeName) {
                case NULL:
                    yamlDesc = ImmutableMap.of("primitive_type", "DT_NULL");
                    break;
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
                    Map charMap = Maps.newHashMap();
                    charMap.put("char", ImmutableMap.of("fixed_length", from.getPrecision()));
                    yamlDesc = ImmutableMap.of("string", charMap);
                    break;
                case VARCHAR:
                    if (from.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
                        Map longTextMap = Maps.newHashMap();
                        longTextMap.put("long_text", ImmutableMap.of());
                        yamlDesc = ImmutableMap.of("string", longTextMap);
                    } else {
                        Map varCharMap = Maps.newHashMap();
                        varCharMap.put(
                                "var_char", ImmutableMap.of("max_length", from.getPrecision()));
                        yamlDesc = ImmutableMap.of("string", varCharMap);
                    }
                    break;
                case DATE:
                    Map dateMap = Maps.newHashMap();
                    dateMap.put("date32", ImmutableMap.of());
                    yamlDesc = ImmutableMap.of("temporal", dateMap);
                    break;
                case TIME:
                    Map timeMap = Maps.newHashMap();
                    timeMap.put("time32", ImmutableMap.of());
                    yamlDesc = ImmutableMap.of("temporal", timeMap);
                    break;
                case TIMESTAMP:
                    Map timestampMap = Maps.newHashMap();
                    timestampMap.put("timestamp", ImmutableMap.of());
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
                                            from.getPrecision()
                                                            == RelDataType.PRECISION_NOT_SPECIFIED
                                                    ? Integer.MAX_VALUE
                                                    : from.getPrecision()));
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
                                    ImmutableMap.of("key_type", keyType, "value_type", valueType));
                    break;
                case DECIMAL:
                    yamlDesc =
                            ImmutableMap.of(
                                    "decimal",
                                    ImmutableMap.of(
                                            "precision", from.getPrecision(),
                                            "scale", from.getScale()));
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "convert RelDataType ["
                                    + from
                                    + "] to GSDataTypeDesc is unsupported yet");
            }
            return new GSDataTypeDesc(yamlDesc);
        }

        private @Nullable Integer getIntValue(Object valueMap, String key) {
            if (valueMap instanceof Map) {
                Object value = ((Map) valueMap).get(key);
                if (value != null) {
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    } else if (value instanceof String) {
                        return Integer.parseInt((String) value);
                    }
                }
            }
            return null;
        }
    }

    class IrCoreOrdinal implements IrDataTypeConvertor<Integer> {
        private final RelDataTypeFactory typeFactory;

        public IrCoreOrdinal(RelDataTypeFactory typeFactory) {
            this.typeFactory = typeFactory;
        }

        @Override
        public RelDataType convert(Integer ordinal) {
            switch (ordinal) {
                case 0:
                    // boolean type
                    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                case 1:
                    // 4-bytes signed integer
                    return typeFactory.createSqlType((SqlTypeName.INTEGER));
                case 2:
                    // 8-bytes signed integer
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case 3:
                    // double precision floating point, 8 bytes
                    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                case 4:
                    // string with unlimited length
                    return typeFactory.createSqlType(
                            SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED);
                case 5:
                    // binary data with unlimited length
                    return typeFactory.createSqlType(SqlTypeName.BINARY);
                case 6:
                    // array of 4-bytes signed integer, unlimited size
                    return typeFactory.createArrayType(
                            typeFactory.createSqlType(SqlTypeName.INTEGER),
                            RelDataType.PRECISION_NOT_SPECIFIED);
                case 7:
                    // array of 8-bytes signed integer, unlimited size
                    return typeFactory.createArrayType(
                            typeFactory.createSqlType(SqlTypeName.BIGINT),
                            RelDataType.PRECISION_NOT_SPECIFIED);
                case 8:
                    // array of double precision floating point, unlimited size
                    return typeFactory.createArrayType(
                            typeFactory.createSqlType(SqlTypeName.DOUBLE),
                            RelDataType.PRECISION_NOT_SPECIFIED);
                case 9:
                    // array of string, unlimited size
                    return typeFactory.createArrayType(
                            typeFactory.createSqlType(
                                    SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED),
                            RelDataType.PRECISION_NOT_SPECIFIED);
                case 12:
                    // int32 days since 1970-01-01
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                case 13:
                    // int32 milliseconds past midnight
                    return typeFactory.createSqlType(SqlTypeName.TIME);
                case 14:
                    // int64 milliseconds since 1970-01-01 00:00:00.000000
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                default:
                    throw new UnsupportedOperationException(
                            "convert IrCoreDataType ["
                                    + ordinal
                                    + "] to RelDataType is unsupported yet");
            }
        }

        @Override
        public Integer convert(RelDataType from) {
            throw new UnsupportedOperationException(
                    "convert RelDataType to IrCoreDataType is unsupported yet");
        }
    }
}
