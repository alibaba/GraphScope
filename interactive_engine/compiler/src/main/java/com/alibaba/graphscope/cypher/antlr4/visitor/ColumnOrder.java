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

package com.alibaba.graphscope.cypher.antlr4.visitor;

import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.base.Objects;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * ColumnOrder keeps fields as the same order with RETURN clause
 */
public class ColumnOrder {
    public static class Field {
        private final RexNode expr;
        private final String alias;

        public Field(RexNode expr, String alias) {
            this.expr = expr;
            this.alias = alias;
        }

        public RexNode getExpr() {
            return expr;
        }

        public String getAlias() {
            return alias;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field = (Field) o;
            return Objects.equal(expr, field.expr) && Objects.equal(alias, field.alias);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(expr, alias);
        }
    }

    public interface FieldSupplier {
        Field get(RelDataType inputType);

        class Default implements FieldSupplier {
            private final GraphBuilder builder;
            private final Supplier<Integer> ordinalSupplier;

            public Default(GraphBuilder builder, Supplier<Integer> ordinalSupplier) {
                this.builder = builder;
                this.ordinalSupplier = ordinalSupplier;
            }

            @Override
            public Field get(RelDataType inputType) {
                String aliasName = inputType.getFieldList().get(ordinalSupplier.get()).getName();
                return new Field(this.builder.variable(aliasName), aliasName);
            }
        }
    }

    private final List<FieldSupplier> fieldSuppliers;

    public ColumnOrder(List<FieldSupplier> fieldSuppliers) {
        this.fieldSuppliers = fieldSuppliers;
    }

    public @Nullable List<Field> getFields(RelDataType inputType) {
        return this.fieldSuppliers.stream().map(k -> k.get(inputType)).collect(Collectors.toList());
    }
}
