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

import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * visit the antlr tree and generate the corresponding {@code RexNode} for path function
 */
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
        this.pathExpand = getPathExpand(parentBuilder.peek(), variable);
        this.innerExpand =
                Objects.requireNonNull(
                        this.pathExpand.getExpand(), "expand in path expand can not be null");
        this.innerGetV =
                Objects.requireNonNull(
                        this.pathExpand.getGetV(), "getV in path expand can not be null");
    }

    private GraphLogicalPathExpand getPathExpand(RelNode top, RexNode variable) {
        int expectedAlias = ((RexGraphVariable) variable).getAliasId();
        List<RelNode> inputQueue = Lists.newArrayList(top);
        while (!inputQueue.isEmpty()) {
            RelNode cur = inputQueue.remove(0);
            if (cur instanceof GraphLogicalPathExpand
                    && (expectedAlias == AliasInference.DEFAULT_ID
                            || ((GraphLogicalPathExpand) cur).getAliasId() == expectedAlias)) {
                return (GraphLogicalPathExpand) cur;
            }
            if (cur instanceof Project) {
                Project project = (Project) cur;
                RelDataType projectType = project.getRowType();
                for (int i = 0; i < projectType.getFieldList().size(); ++i) {
                    RelDataTypeField field = projectType.getFieldList().get(i);
                    if (field.getIndex() == expectedAlias) {
                        if (i < project.getProjects().size()
                                && project.getProjects().get(i) instanceof RexGraphVariable) {
                            return getPathExpand(project.getInput(), project.getProjects().get(i));
                        }
                        break;
                    }
                }
            } else if (cur instanceof CommonTableScan) {
                CommonOptTable optTable = (CommonOptTable) ((CommonTableScan) cur).getTable();
                return getPathExpand(optTable.getCommon(), variable);
            } else if (cur instanceof GraphLogicalSingleMatch) {
                return getPathExpand(((GraphLogicalSingleMatch) cur).getSentence(), variable);
            } else if (cur instanceof GraphLogicalMultiMatch) {
                List<RelNode> sentences = ((GraphLogicalMultiMatch) cur).getSentences();
                for (RelNode sentence : sentences) {
                    try {
                        return getPathExpand(sentence, variable);
                    } catch (IllegalArgumentException e) {
                        // ignore
                    }
                }
            }
            if (AliasInference.removeAlias(cur)) {
                break;
            }
            inputQueue.addAll(cur.getInputs());
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
        GraphOpt.PathExpandFunction funcOpt = getFuncOpt();
        // avoid to throw exception if the function opt is VERTEX_EDGE, to support the semantics
        // like
        // g.V().out('2..3').with('RESULT_OPT', 'ALL_V_E').valueMap('name', 'weight'),
        // in nested path collection, each edge will return the property of 'weight' while each
        // vertex will return the property of 'name'
        boolean throwsOnPropertyNotFound =
                (funcOpt == GraphOpt.PathExpandFunction.VERTEX_EDGE) ? false : true;
        RexNode propertyProjection =
                propertyProjection(
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerExpand),
                                                builder.variable((String) null),
                                                throwsOnPropertyNotFound))
                                        .visitTraversalMethod_valueMap(ctx),
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerGetV),
                                                builder.variable((String) null),
                                                throwsOnPropertyNotFound))
                                        .visitTraversalMethod_valueMap(ctx),
                        builder);
        return builder.call(
                GraphStdOperatorTable.PATH_FUNCTION,
                variable,
                builder.getRexBuilder().makeFlag(funcOpt),
                propertyProjection);
    }

    @Override
    public RexNode visitTraversalMethod_values(GremlinGSParser.TraversalMethod_valuesContext ctx) {
        GraphBuilder builder =
                GraphBuilder.create(
                        parentBuilder.getContext(),
                        (GraphOptCluster) parentBuilder.getCluster(),
                        parentBuilder.getRelOptSchema());
        GraphOpt.PathExpandFunction funcOpt = getFuncOpt();
        boolean throwsOnPropertyNotFound =
                (funcOpt == GraphOpt.PathExpandFunction.VERTEX_EDGE) ? false : true;
        RexNode propertyProjection =
                propertyProjection(
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerExpand),
                                                builder.variable((String) null),
                                                throwsOnPropertyNotFound))
                                        .visitTraversalMethod_values(ctx),
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerGetV),
                                                builder.variable((String) null),
                                                throwsOnPropertyNotFound))
                                        .visitTraversalMethod_values(ctx),
                        builder);
        return builder.call(
                GraphStdOperatorTable.PATH_FUNCTION,
                variable,
                builder.getRexBuilder().makeFlag(funcOpt),
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
        GraphOpt.PathExpandFunction funcOpt = getFuncOpt();
        boolean throwsOnPropertyNotFound =
                (funcOpt == GraphOpt.PathExpandFunction.VERTEX_EDGE) ? false : true;
        RexNode propertyProjection =
                propertyProjection(
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerExpand),
                                                builder.variable((String) null),
                                                throwsOnPropertyNotFound))
                                        .visitTraversalMethod_elementMap(ctx),
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerGetV),
                                                builder.variable((String) null),
                                                throwsOnPropertyNotFound))
                                        .visitTraversalMethod_elementMap(ctx),
                        builder);
        return builder.call(
                GraphStdOperatorTable.PATH_FUNCTION,
                variable,
                builder.getRexBuilder().makeFlag(funcOpt),
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
        GraphOpt.PathExpandFunction funcOpt = getFuncOpt();
        boolean throwsOnPropertyNotFound =
                (funcOpt == GraphOpt.PathExpandFunction.VERTEX_EDGE) ? false : true;
        RexNode propertyProjection =
                propertyProjection(
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerExpand),
                                                builder.variable((String) null),
                                                throwsOnPropertyNotFound))
                                        .visitTraversalMethod_selectby(ctx),
                        () ->
                                (new ExpressionVisitor(
                                                builder.push(innerGetV),
                                                builder.variable((String) null),
                                                throwsOnPropertyNotFound))
                                        .visitTraversalMethod_selectby(ctx),
                        builder);
        return builder.call(
                GraphStdOperatorTable.PATH_FUNCTION,
                variable,
                builder.getRexBuilder().makeFlag(funcOpt),
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
        if (expandProjection == null || getVProjection == null) {
            Preconditions.checkArgument(
                    expandProjection != null || getVProjection != null,
                    "invalid query given properties, not found in path expand");
            return expandProjection != null ? expandProjection : getVProjection;
        }
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
            Preconditions.checkArgument(
                    !newOperands.isEmpty(),
                    "invalid query given properties, not found in path expand");
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
