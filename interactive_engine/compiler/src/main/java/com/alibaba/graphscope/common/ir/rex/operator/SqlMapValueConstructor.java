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

package com.alibaba.graphscope.common.ir.rex.operator;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class SqlMapValueConstructor extends SqlMultisetValueConstructor {
    public SqlMapValueConstructor() {
        super("MAP", SqlKind.MAP_VALUE_CONSTRUCTOR);
    }

    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        Pair<RelDataType, RelDataType> type =
                getComponentTypes(opBinding.getTypeFactory(), opBinding.collectOperandTypes());
        return SqlTypeUtil.createMapType(opBinding.getTypeFactory(), type.left, type.right, false);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        List<RelDataType> argTypes = callBinding.collectOperandTypes();
        if (argTypes.size() % 2 > 0) {
            throw callBinding.newValidationError(Static.RESOURCE.mapRequiresEvenArgCount());
        }
        return true;
    }

    private static Pair<@Nullable RelDataType, @Nullable RelDataType> getComponentTypes(
            RelDataTypeFactory typeFactory, List<RelDataType> argTypes) {
        RelDataType leftType, rightType;
        if (argTypes.isEmpty()) {
            leftType = typeFactory.createSqlType(SqlTypeName.ANY);
            rightType = typeFactory.createSqlType(SqlTypeName.ANY);
        } else {
            leftType = typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 0));
            rightType = typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 1));
            if (leftType == null) {
                leftType = typeFactory.createSqlType(SqlTypeName.ANY);
            }
            if (rightType == null) {
                rightType = typeFactory.createSqlType(SqlTypeName.ANY);
            }
        }
        return Pair.of(leftType, rightType);
    }
}
