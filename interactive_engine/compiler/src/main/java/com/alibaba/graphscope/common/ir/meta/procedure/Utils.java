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

import com.google.common.base.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public class Utils {
    public static String typeToStr(RelDataType dataType) {
        SqlTypeName typeName = dataType.getSqlTypeName();
        if (typeName == SqlTypeName.CHAR) {
            return "STRING";
        } else if (typeName == SqlTypeName.BIGINT) {
            return "LONG";
        } else if (typeName == SqlTypeName.ARRAY || typeName == SqlTypeName.MULTISET) {
            return String.format(
                    "%s(%s)", typeName.getName(), typeToStr(dataType.getComponentType()));
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
            RelDataType componentType = strToType(getComponentTypeStr(typeStr), typeFactory);
            return typeFactory.createArrayType(componentType, -1);
        } else if (typeStr.startsWith(SqlTypeName.MULTISET.getName())) {
            RelDataType componentType = strToType(getComponentTypeStr(typeStr), typeFactory);
            return typeFactory.createMultisetType(componentType, -1);
        } else {
            return typeFactory.createSqlType(SqlTypeName.valueOf(typeStr));
        }
    }

    private static String getComponentTypeStr(String typeStr) {
        int leftBraceIdx = typeStr.indexOf("(");
        int rightBraceIdx = typeStr.indexOf(")");
        Preconditions.checkArgument(
                leftBraceIdx != -1 && rightBraceIdx != -1, "invalid type pattern " + typeStr);
        return typeStr.substring(leftBraceIdx + 1, rightBraceIdx);
    }
}
