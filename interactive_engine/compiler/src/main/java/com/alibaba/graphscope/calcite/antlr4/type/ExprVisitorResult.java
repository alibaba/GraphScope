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

package com.alibaba.graphscope.calcite.antlr4.type;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Objects;

/**
 * In common cases, the result object returned by {@code ExpressionVisitor} is a simple {@code RexNode},
 * but there exists more complex situations:
 * there is as least one aggregate call in an expression, i.e. count(n.age) + 2, count(n.age) + sum(n.age).
 * In real-execution, count(n.age) or sum(n.age) need to be calculated first, which the latter project is based on.
 * {@code ExpressionVisitor} will return a list of aggregate calls (aggregate in advance) with a project expression to handle the case.
 */
public class ExprVisitorResult {
    private final List<RelBuilder.AggCall> aggCalls;
    private final RexNode expr;

    // i.e. count(n.name) + 1 -> count(n.name) as a, project(a + 1)
    public ExprVisitorResult(List<RelBuilder.AggCall> firstCalls, RexNode expr) {
        this.aggCalls = Objects.requireNonNull(firstCalls);
        this.expr = Objects.requireNonNull(expr);
    }

    public ExprVisitorResult(RexNode expr) {
        this(ImmutableList.of(), expr);
    }

    public List<RelBuilder.AggCall> getAggCalls() {
        return aggCalls;
    }

    public RexNode getExpr() {
        return expr;
    }
}
