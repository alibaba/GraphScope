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
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * maintain each aggregate function with alias
 */
public class GraphAggCall implements RelBuilder.AggCall {
    private final RelDataType type;
    // primary parameters
    private final RelOptCluster cluster;
    private final List<RexNode> operands; // may be empty
    private final SqlAggFunction aggFunction;
    // optional parameters
    private @Nullable String alias;
    private boolean distinct;
    private boolean approximate;
    private boolean ignoreNulls;
    private @Nullable RexNode filter;
    private @Nullable ImmutableList<RexNode> distinctKeys;
    private @Nullable ImmutableList<RexNode> orderKeys;

    public GraphAggCall(RelOptCluster cluster, SqlAggFunction aggFunction, List<RexNode> operands) {
        this.cluster = Objects.requireNonNull(cluster);
        this.aggFunction = aggFunction;
        this.operands = ObjectUtils.requireNonEmpty(operands);
        this.type = validateThenDerive(aggFunction, operands);
    }

    @Override
    public GraphAggCall as(@Nullable String alias) {
        this.alias = alias;
        return this;
    }

    @Override
    public GraphAggCall distinct(boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    @Override
    public GraphAggCall approximate(boolean approximate) {
        this.approximate = approximate;
        return this;
    }

    @Override
    public GraphAggCall ignoreNulls(boolean ignoreNulls) {
        this.ignoreNulls = ignoreNulls;
        return this;
    }

    @Override
    public GraphAggCall filter(RexNode rexNode) {
        this.filter = Objects.requireNonNull(rexNode);
        return this;
    }

    @Override
    public GraphAggCall sort(Iterable<RexNode> orderKeys) {
        this.orderKeys = ImmutableList.copyOf(ObjectUtils.requireNonEmpty(orderKeys));
        return this;
    }

    @Override
    public GraphAggCall unique(@Nullable Iterable<RexNode> distinctKeys) {
        this.distinctKeys = ImmutableList.copyOf(ObjectUtils.requireNonEmpty(distinctKeys));
        return this;
    }

    @Override
    public RelBuilder.OverCall over() {
        throw new UnsupportedOperationException("over in AggCall is unsupported yet");
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        return builder.append("{")
                .append("operands=")
                .append(operands)
                .append(", aggFunction=")
                .append(aggFunction)
                .append(", alias='")
                .append(AliasInference.SIMPLE_NAME(alias))
                .append('\'')
                .append(", distinct=")
                .append(distinct)
                .append('}')
                .toString();
    }

    private RelDataType validateThenDerive(SqlAggFunction aggFunction, List<RexNode> operands) {
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

    public List<RexNode> getOperands() {
        return Collections.unmodifiableList(operands);
    }

    public SqlAggFunction getAggFunction() {
        return aggFunction;
    }

    public @Nullable String getAlias() {
        return alias;
    }

    public RexNode rexCall() {
        RexBuilder rexBuilder = this.cluster.getRexBuilder();
        return rexBuilder.makeCall(this.type, aggFunction, operands);
    }

    public RelOptCluster getCluster() {
        return cluster;
    }

    public GraphAggCall copy(String alias) {
        return new GraphAggCall(this.cluster, this.aggFunction, this.operands)
                .as(alias)
                .distinct(this.distinct);
    }
}
