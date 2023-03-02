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

package com.alibaba.graphscope.common.ir.rex;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.*;

import java.util.List;

/**
 * To inherit from {@code SqlCallBinding},
 * we have to pass arguments of {@code SqlValidator} and {@code SqlCall} even though they will never be used,
 * we prepare some default arguments to satisfy the constructor parameter lists of {@code SqlCallBinding} in this class
 */
public abstract class AbstractCallBinding extends SqlCallBinding {
    private static final CalciteSchema DEFAULT_ROOT_SCHEMA =
            CalciteSchema.createRootSchema(false, false, "CATALOG");
    private static final List<String> DEFAULT_SUB_SCHEMA = ImmutableList.of();
    private static final List<SqlNode> DEFAULT_OPERANDS = ImmutableList.of();

    public AbstractCallBinding(RelDataTypeFactory typeFactory, SqlOperator operator) {
        super(
                SqlValidatorUtil.newValidator(
                        SqlStdOperatorTable.instance(),
                        new CalciteCatalogReader(
                                DEFAULT_ROOT_SCHEMA, DEFAULT_SUB_SCHEMA, typeFactory, null),
                        typeFactory,
                        SqlValidator.Config.DEFAULT),
                null,
                new SqlBasicCall(operator, DEFAULT_OPERANDS, SqlParserPos.QUOTED_ZERO));
    }

    // override SqlCallBinding
    @Override
    public boolean isTypeCoercionEnabled() {
        return false;
    }
}
