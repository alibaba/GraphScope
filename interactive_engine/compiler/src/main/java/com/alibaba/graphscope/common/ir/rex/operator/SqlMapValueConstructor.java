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

import com.alibaba.graphscope.common.ir.rex.RexCallBinding;
import com.alibaba.graphscope.common.ir.type.ArbitraryMapType;
import com.alibaba.graphscope.common.ir.type.GraphTypeFactoryImpl;
import com.google.common.collect.Maps;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;

/**
 * This operator is used to fold columns into a map, i.e. {name: a.name, age: a.age} in cypher queries.
 * However, original implementation in calcite does not allow expressions with different types as its operands, so we extend the superclass to support the situation.
 * If the expressions do not have a least-restrictive type, then the derived type will be {@code SqlTypeName.ANY}.
 */
public class SqlMapValueConstructor extends SqlMultisetValueConstructor {
    public SqlMapValueConstructor() {
        super("MAP", SqlKind.MAP_VALUE_CONSTRUCTOR);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        List<RelDataType> argTypes = opBinding.collectOperandTypes();
        List<RelDataType> keyTypes = Util.quotientList(argTypes, 2, 0);
        List<RelDataType> valueTypes = Util.quotientList(argTypes, 2, 1);
        RelDataType keyType = getComponentType(typeFactory, keyTypes);
        RelDataType valueType = getComponentType(typeFactory, valueTypes);
        if (keyType != null
                && keyType.getSqlTypeName() != SqlTypeName.ANY
                && valueType != null
                && valueType.getSqlTypeName() != SqlTypeName.ANY) {
            return SqlTypeUtil.createMapType(opBinding.getTypeFactory(), keyType, valueType, false);
        } else {
            Map<RexNode, ArbitraryMapType.KeyValueType> keyValueTypeMap = Maps.newHashMap();
            List<RexNode> operands = ((RexCallBinding) opBinding).getRexOperands();
            for (int i = 0; i < operands.size(); i += 2) {
                keyValueTypeMap.put(
                        operands.get(i),
                        new ArbitraryMapType.KeyValueType(
                                keyTypes.get(i / 2), valueTypes.get(i / 2)));
            }
            return ((GraphTypeFactoryImpl) typeFactory)
                    .createArbitraryMapType(keyValueTypeMap, false);
        }
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        List<RelDataType> argTypes = callBinding.collectOperandTypes();
        if (argTypes.size() % 2 > 0) {
            throw callBinding.newValidationError(Static.RESOURCE.mapRequiresEvenArgCount());
        }
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
