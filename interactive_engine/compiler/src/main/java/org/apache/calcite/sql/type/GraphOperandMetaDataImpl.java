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

import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

public class GraphOperandMetaDataImpl extends GraphFamilyOperandTypeChecker
        implements SqlOperandMetadata {
    private final Function<RelDataTypeFactory, List<RelDataType>> paramTypesFactory;
    private final IntFunction<String> paramNameFn;

    GraphOperandMetaDataImpl(
            List<SqlTypeFamily> families,
            Function<RelDataTypeFactory, List<RelDataType>> paramTypesFactory,
            IntFunction<String> paramNameFn,
            Predicate<Integer> optional) {
        super(families, optional);
        this.paramTypesFactory = Objects.requireNonNull(paramTypesFactory, "paramTypesFactory");
        this.paramNameFn = paramNameFn;
    }

    @Override
    public boolean isFixedParameters() {
        return true;
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
        return (List) this.paramTypesFactory.apply(typeFactory);
    }

    @Override
    public List<String> paramNames() {
        return Functions.generate(this.families.size(), this.paramNameFn);
    }
}
