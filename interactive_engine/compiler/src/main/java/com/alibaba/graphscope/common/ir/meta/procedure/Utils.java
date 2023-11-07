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

package com.alibaba.graphscope.common.ir.meta.procedure;

import com.alibaba.graphscope.common.ir.type.ArbitraryArrayType;
import com.alibaba.graphscope.common.ir.type.ArbitraryMapType;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.stream.Collectors;

public class Utils {
    public static String typeToStr(RelDataType dataType) {
        SqlTypeName typeName = dataType.getSqlTypeName();
        if (typeName == SqlTypeName.CHAR) {
            return "STRING";
        } else if (typeName == SqlTypeName.BIGINT) {
            return "LONG";
        } else if (typeName == SqlTypeName.ARRAY || typeName == SqlTypeName.MULTISET) {
            if (dataType instanceof ArbitraryArrayType) {
                List<RelDataType> componentTypes =
                        ((ArbitraryArrayType) dataType).getComponentTypes();
                StringBuilder sb = new StringBuilder();
                sb.append(typeName.getName() + "(");
                for (int i = 0; i < componentTypes.size(); i++) {
                    sb.append(typeToStr(componentTypes.get(i)));
                    if (i != componentTypes.size() - 1) {
                        sb.append(",");
                    }
                }
                sb.append(")");
                return sb.toString();
            } else {
                return String.format(
                        "%s(%s)", typeName.getName(), typeToStr(dataType.getComponentType()));
            }
        } else if (typeName == SqlTypeName.MAP) {
            if (dataType instanceof ArbitraryMapType) {
                List<RelDataType> keyTypes = ((ArbitraryMapType) dataType).getKeyTypes();
                List<RelDataType> valueTypes = ((ArbitraryMapType) dataType).getValueTypes();
                Preconditions.checkArgument(
                        keyTypes.size() == valueTypes.size(),
                        "key size and value size are not equal in " + dataType);
                StringBuilder sb = new StringBuilder();
                sb.append(typeName.getName() + "(");
                for (int i = 0; i < keyTypes.size(); i++) {
                    sb.append(typeToStr(keyTypes.get(i)));
                    sb.append(",");
                    sb.append(typeToStr(valueTypes.get(i)));
                    if (i != keyTypes.size() - 1) {
                        sb.append(",");
                    }
                }
                sb.append(")");
                return sb.toString();
            } else {
                return String.format(
                        "%s(%s,%s)",
                        typeName.getName(),
                        typeToStr(dataType.getKeyType()),
                        typeToStr(dataType.getValueType()));
            }
        } else {
            // todo: convert vertex or edge type to string
            return typeName.getName();
        }
    }

    public static RelDataType strToType(String typeStr, RelDataTypeFactory typeFactory) {
        typeStr = typeStr.toUpperCase();
        if (typeStr.equals("STRING")) {
            return typeFactory.createSqlType(SqlTypeName.CHAR);
        } else if (typeStr.equals("LONG")) {
            return typeFactory.createSqlType(SqlTypeName.BIGINT);
        } else if (typeStr.startsWith(SqlTypeName.ARRAY.getName())) {
            List<String> componentTypeStr = getComponentTypeStr(typeStr);
            if (componentTypeStr.size() == 1) {
                RelDataType componentType = strToType(componentTypeStr.get(0), typeFactory);
                return typeFactory.createArrayType(componentType, -1);
            } else {
                List<RelDataType> componentTypes =
                        componentTypeStr.stream()
                                .map(k -> strToType(k, typeFactory))
                                .collect(Collectors.toList());
                return ((GraphTypeFactoryImpl) typeFactory)
                        .createArbitraryArrayType(componentTypes, false);
            }
        } else if (typeStr.startsWith(SqlTypeName.MAP.getName())) {
            List<String> componentTypeStr = getComponentTypeStr(typeStr);
            if (componentTypeStr.size() == 2) {
                RelDataType keyType = strToType(componentTypeStr.get(0), typeFactory);
                RelDataType valueType = strToType(componentTypeStr.get(1), typeFactory);
                return typeFactory.createMapType(keyType, valueType);
            } else {
                List<RelDataType> keyTypes = Lists.newArrayList();
                List<RelDataType> valueTypes = Lists.newArrayList();
                for (int i = 0; i < componentTypeStr.size(); i++) {
                    if ((i & 1) == 0) {
                        keyTypes.add(strToType(componentTypeStr.get(i), typeFactory));
                    } else {
                        valueTypes.add(strToType(componentTypeStr.get(i), typeFactory));
                    }
                }
                return ((GraphTypeFactoryImpl) typeFactory)
                        .createArbitraryMapType(keyTypes, valueTypes, false);
            }
        } else if (typeStr.startsWith(SqlTypeName.MULTISET.getName())) {
            RelDataType componentType = strToType(getComponentTypeStr(typeStr).get(0), typeFactory);
            return typeFactory.createMultisetType(componentType, -1);
        } else {
            return typeFactory.createSqlType(SqlTypeName.valueOf(typeStr));
        }
    }

    private static List<String> getComponentTypeStr(String typeStr) {
        int leftBraceIdx = typeStr.indexOf("(");
        int rightBraceIdx = typeStr.lastIndexOf(")");
        Preconditions.checkArgument(
                leftBraceIdx != -1 && rightBraceIdx != -1, "invalid type pattern " + typeStr);
        return com.alibaba.graphscope.common.config.Utils.convertDotString(
                typeStr.substring(leftBraceIdx + 1, rightBraceIdx));
    }
}
