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

package org.apache.calcite.sql.type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class GraphOperandMetaDataImpl extends GraphFamilyOperandTypeChecker
        implements SqlOperandMetadata {
    private final Function<RelDataTypeFactory, List<RelDataType>> paramTypesFactory;
    private final IntFunction<String> paramNameFn;

    GraphOperandMetaDataImpl(
            List<SqlTypeFamily> families,
            Function<@Nullable RelDataTypeFactory, List<RelDataType>> paramTypesFactory,
            IntFunction<String> paramNameFn,
            Predicate<Integer> optional) {
        super(families, optional);
        this.paramTypesFactory = Objects.requireNonNull(paramTypesFactory, "paramTypesFactory");
        this.paramNameFn = paramNameFn;
    }

    @Override
    protected Collection<SqlTypeName> getAllowedTypeNames(
            SqlTypeFamily family, int iFormalOperand) {
        List<RelDataType> paramsAllowedTypes = paramTypes(null);
        Preconditions.checkArgument(
                paramsAllowedTypes.size() > iFormalOperand,
                "cannot find allowed type for type index="
                        + iFormalOperand
                        + " from the allowed types list="
                        + paramsAllowedTypes);
        return ImmutableList.of(paramsAllowedTypes.get(iFormalOperand).getSqlTypeName());
    }

    @Override
    public boolean isFixedParameters() {
        return true;
    }

    @Override
    public List<RelDataType> paramTypes(@Nullable RelDataTypeFactory typeFactory) {
        return this.paramTypesFactory.apply(typeFactory);
    }

    @Override
    public List<String> paramNames() {
        return Functions.generate(this.families.size(), this.paramNameFn);
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
        return SqlUtil.getAliasedSignature(
                op,
                opName,
                paramTypes(null).stream()
                        .map(k -> k.getSqlTypeName())
                        .collect(Collectors.toList()));
    }
}
