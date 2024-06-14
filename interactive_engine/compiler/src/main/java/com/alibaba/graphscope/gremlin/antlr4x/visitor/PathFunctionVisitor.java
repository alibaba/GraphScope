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

package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.alibaba.graphscope.grammar.GremlinGSBaseVisitor;
import com.alibaba.graphscope.grammar.GremlinGSParser;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PathFunctionVisitor extends GremlinGSBaseVisitor<RexNode> {
    private final GraphBuilder parentBuilder;
    private final RexNode variable;
    private final GraphLogicalPathExpand pathExpand;
    private final RelNode innerExpand;
    private final RelNode innerGetV;

    public PathFunctionVisitor(GraphBuilder parentBuilder, RexNode variable) {
        Preconditions.checkArgument(
                variable instanceof RexGraphVariable
                        && ((RexGraphVariable) variable).getProperty() == null
                        && variable.getType() instanceof GraphPathType,
                "variable [" + variable + "] can not denote a path expand");
        this.parentBuilder = parentBuilder;
        this.variable = variable;
        this.pathExpand = getPathExpand(variable);
        this.innerExpand =
                Objects.requireNonNull(
                        this.pathExpand.getExpand(), "expand in path expand can not be null");
        this.innerGetV =
                Objects.requireNonNull(
                        this.pathExpand.getGetV(), "getV in path expand can not be null");
    }

    private GraphLogicalPathExpand getPathExpand(RexNode variable) {
        int expectedAlias = ((RexGraphVariable) variable).getAliasId();
        for (int inputOrdinal = 0; inputOrdinal < parentBuilder.size(); ++inputOrdinal) {
            List<RelNode> inputQueue = Lists.newArrayList(parentBuilder.peek(inputOrdinal));
            while (!inputQueue.isEmpty()) {
                RelNode cur = inputQueue.remove(0);
                if (cur instanceof GraphLogicalPathExpand
                        && (expectedAlias == AliasInference.DEFAULT_ID
                                || ((GraphLogicalPathExpand) cur).getAliasId() == expectedAlias)) {
                    return (GraphLogicalPathExpand) cur;
                }
                if (AliasInference.removeAlias(cur)) {
                    break;
                }
                inputQueue.addAll(cur.getInputs());
            }
        }
        throw new IllegalArgumentException("can not find path expand by variable " + variable);
    }

    @Override
    public RexNode visitTraversalMethod_valueMap(
            GremlinGSParser.TraversalMethod_valueMapContext ctx) {
        GraphBuilder builder =
                GraphBuilder.create(
                        parentBuilder.getContext(),
                        (GraphOptCluster) parentBuilder.getCluster(),
                        parentBuilder.getRelOptSchema());
        RexNode propertyProjection =
                propertyProjection(
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerExpand),
                                                builder.variable((String) null)))
                                        .visitTraversalMethod_valueMap(ctx),
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerGetV),
                                                builder.variable((String) null)))
                                        .visitTraversalMethod_valueMap(ctx),
                        builder);
        return builder.call(
                GraphStdOperatorTable.PATH_FUNCTION,
                variable,
                builder.getRexBuilder().makeFlag(getFuncOpt()),
                propertyProjection);
    }

    @Override
    public RexNode visitTraversalMethod_values(GremlinGSParser.TraversalMethod_valuesContext ctx) {
        GraphBuilder builder =
                GraphBuilder.create(
                        parentBuilder.getContext(),
                        (GraphOptCluster) parentBuilder.getCluster(),
                        parentBuilder.getRelOptSchema());
        RexNode propertyProjection =
                propertyProjection(
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerExpand),
                                                builder.variable((String) null)))
                                        .visitTraversalMethod_values(ctx),
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerGetV),
                                                builder.variable((String) null)))
                                        .visitTraversalMethod_values(ctx),
                        builder);
        return builder.call(
                GraphStdOperatorTable.PATH_FUNCTION,
                variable,
                builder.getRexBuilder().makeFlag(getFuncOpt()),
                propertyProjection);
    }

    @Override
    public RexNode visitTraversalMethod_elementMap(
            GremlinGSParser.TraversalMethod_elementMapContext ctx) {
        GraphBuilder builder =
                GraphBuilder.create(
                        parentBuilder.getContext(),
                        (GraphOptCluster) parentBuilder.getCluster(),
                        parentBuilder.getRelOptSchema());
        RexNode propertyProjection =
                propertyProjection(
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerExpand),
                                                builder.variable((String) null)))
                                        .visitTraversalMethod_elementMap(ctx),
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerGetV),
                                                builder.variable((String) null)))
                                        .visitTraversalMethod_elementMap(ctx),
                        builder);
        return builder.call(
                GraphStdOperatorTable.PATH_FUNCTION,
                variable,
                builder.getRexBuilder().makeFlag(getFuncOpt()),
                propertyProjection);
    }

    @Override
    public RexNode visitTraversalMethod_selectby(
            GremlinGSParser.TraversalMethod_selectbyContext ctx) {
        GraphBuilder builder =
                GraphBuilder.create(
                        parentBuilder.getContext(),
                        (GraphOptCluster) parentBuilder.getCluster(),
                        parentBuilder.getRelOptSchema());
        RexNode propertyProjection =
                propertyProjection(
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerExpand),
                                                builder.variable((String) null)))
                                        .visitTraversalMethod_selectby(ctx),
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerGetV),
                                                builder.variable((String) null)))
                                        .visitTraversalMethod_selectby(ctx),
                        builder);
        return builder.call(
                GraphStdOperatorTable.PATH_FUNCTION,
                variable,
                builder.getRexBuilder().makeFlag(getFuncOpt()),
                propertyProjection);
    }

    private RexNode propertyProjection(
            Supplier<RexNode> expandSupplier,
            Supplier<RexNode> getVSupplier,
            GraphBuilder builder) {
        GraphOpt.PathExpandFunction funcOpt = getFuncOpt();
        switch (funcOpt) {
            case VERTEX:
                return getVSupplier.get();
            case EDGE:
                return expandSupplier.get();
            case VERTEX_EDGE:
            default:
                RexNode expandProjection = expandSupplier.get();
                RexNode getVProjection = getVSupplier.get();
                return unionProjection(expandProjection, getVProjection, builder);
        }
    }

    private RexNode unionProjection(
            RexNode expandProjection, RexNode getVProjection, GraphBuilder builder) {
        Preconditions.checkArgument(
                expandProjection.getKind() == getVProjection.getKind(),
                "expand projection ["
                        + expandProjection
                        + "] is not consistent with getV projection ["
                        + getVProjection
                        + "]");
        if (expandProjection instanceof RexCall) {
            RexCall expandCall = (RexCall) expandProjection;
            RexCall getVCall = (RexCall) getVProjection;
            List<RexNode> newOperands = Lists.newArrayList(expandCall.getOperands());
            newOperands.addAll(getVCall.getOperands());
            return builder.call(
                    expandCall.getOperator(),
                    newOperands.stream().distinct().collect(Collectors.toList()));
        }
        return expandProjection;
    }

    private GraphOpt.PathExpandFunction getFuncOpt() {
        switch (pathExpand.getResultOpt()) {
            case ALL_V:
            case END_V:
                return GraphOpt.PathExpandFunction.VERTEX;
            case ALL_V_E:
            default:
                return GraphOpt.PathExpandFunction.VERTEX_EDGE;
        }
    }
}
