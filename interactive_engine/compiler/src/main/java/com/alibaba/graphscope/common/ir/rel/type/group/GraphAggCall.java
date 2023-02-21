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

package com.alibaba.graphscope.common.ir.rel.type.group;

import com.alibaba.graphscope.common.ir.rex.RexCallBinding;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.Litmus;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * maintain each aggregate function with alias
 */
public class GraphAggCall extends AbstractAggCall {
    private final RelDataType type;
    private final boolean distinct;
    private final ImmutableList<RexNode> operands; // may be empty
    private final SqlAggFunction aggFunction;
    private final @Nullable String alias;
    private final RelOptCluster cluster;

    public GraphAggCall(
            RelOptCluster cluster,
            SqlAggFunction aggFunction,
            boolean distinct,
            @Nullable String alias,
            ImmutableList<RexNode> operands) {
        this.cluster = Objects.requireNonNull(cluster);
        this.aggFunction = aggFunction;
        this.distinct = distinct;
        this.alias = alias;
        this.operands = operands;
        this.type = validateThenDerive(aggFunction, operands);
    }

    private RelDataType validateThenDerive(
            SqlAggFunction aggFunction, ImmutableList<RexNode> operands) {
        if (cluster != null) {
            RelDataTypeFactory factory = cluster.getTypeFactory();
            RexCallBinding callBinding =
                    new RexCallBinding(factory, aggFunction, operands, ImmutableList.of());
            // check count of operands, if fail throw exceptions
            aggFunction.validRexOperands(callBinding.getOperandCount(), Litmus.THROW);
            // check type of each operand, if fail throw exceptions
            aggFunction.checkOperandTypes(callBinding, true);
            return aggFunction.inferReturnType(callBinding);
        } else {
            return null;
        }
    }

    public RelDataType getType() {
        return type;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public ImmutableList<RexNode> getOperands() {
        return operands;
    }

    public SqlAggFunction getAggFunction() {
        return aggFunction;
    }

    public String getAlias() {
        return alias;
    }

    public RexNode rexCall() {
        RexBuilder rexBuilder = this.cluster.getRexBuilder();
        return rexBuilder.makeCall(this.type, aggFunction, operands);
    }

    public RelOptCluster getCluster() {
        return cluster;
    }

    @Override
    public String toString() {
        return "{"
                + "operands="
                + operands
                + ", aggFunction="
                + aggFunction
                + ", alias='"
                + alias
                + '\''
                + '}';
    }
}
