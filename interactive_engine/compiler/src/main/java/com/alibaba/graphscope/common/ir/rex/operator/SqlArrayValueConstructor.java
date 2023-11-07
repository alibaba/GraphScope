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

import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * This operator is used to fold multiple columns into a single array, i.e. [a.name, a.age] in cypher queries.
 * However, original implementation in calcite does not allow expressions with different types as its operands, so we extend the superclass to support the situation.
 * If the expressions do not have a least-restrictive type, then the derived type will be {@code SqlTypeName.ANY}.
 */
public class SqlArrayValueConstructor extends SqlMultisetValueConstructor {
    public SqlArrayValueConstructor() {
        super("ARRAY", SqlKind.ARRAY_VALUE_CONSTRUCTOR);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        List<RelDataType> argTypes = opBinding.collectOperandTypes();
        RelDataType componentType = getComponentType(typeFactory, argTypes);
        if (componentType != null && componentType.getSqlTypeName() != SqlTypeName.ANY) {
            return SqlTypeUtil.createArrayType(typeFactory, componentType, false);
        } else {
            return ((GraphTypeFactoryImpl) typeFactory).createArbitraryArrayType(argTypes, false);
        }
    }

    // operands of array value constructor can be any, even if empty, i.e []
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }

    @Override
    protected @Nullable RelDataType getComponentType(
            RelDataTypeFactory typeFactory, List<RelDataType> argTypes) {
        try {
            RelDataType componentType = typeFactory.leastRestrictive(argTypes);
            return (componentType == null)
                    ? typeFactory.createSqlType(SqlTypeName.ANY)
                    : componentType;
        } catch (Throwable e) {
            return typeFactory.createSqlType(SqlTypeName.ANY);
        }
    }
}
