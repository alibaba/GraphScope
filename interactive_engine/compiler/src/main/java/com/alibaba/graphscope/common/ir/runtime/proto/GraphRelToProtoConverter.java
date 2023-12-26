/*
 * Copyright 2023 Alibaba Group Holding Limited.
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

package com.alibaba.graphscope.common.ir.runtime.proto;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rel.type.order.GraphFieldCollation;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.runtime.type.PhysicalNode;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt.PhysicalGetVOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.gaia.proto.GraphAlgebra;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GraphRelToProtoConverter extends GraphShuttle {
    private static final Logger logger = LoggerFactory.getLogger(GraphRelToProtoConverter.class);
    private final boolean isColumnId;
    private final RexBuilder rexBuilder;
    private final Configs graphConfig;
    private GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder;

    public GraphRelToProtoConverter(
            boolean isColumnId,
            Configs configs,
            GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder) {
        this.isColumnId = isColumnId;
        this.rexBuilder = GraphPlanner.rexBuilderFactory.apply(configs);
        this.graphConfig = configs;
        this.physicalBuilder = physicalBuilder;
    }

    @Override
    public RelNode visit(GraphLogicalSource source) {
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Scan.Builder scanBuilder = GraphAlgebraPhysical.Scan.newBuilder();
        GraphAlgebra.IndexPredicate indexPredicate = buildIndexPredicates(source);
        if (indexPredicate != null) {
            scanBuilder.setIdxPredicate(indexPredicate);
        }
        scanBuilder.setParams(buildQueryParams(source));
        scanBuilder.setAlias(Utils.asAliasId(source.getAliasId()));
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setScan(scanBuilder));
        oprBuilder.addAllMetaData(Utils.physicalProtoRowType(source.getRowType(), isColumnId));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(source, oprBuilder.build());
    }

    @Override
    public RelNode visit(GraphLogicalExpand expand) {
        visitChildren(expand);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.EdgeExpand.Builder edgeExpand = buildEdgeExpand(expand);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setEdge(edgeExpand));
        oprBuilder.addAllMetaData(Utils.physicalProtoRowType(expand.getRowType(), isColumnId));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(expand, oprBuilder.build());
    }

    @Override
    public RelNode visit(GraphLogicalGetV getV) {
        visitChildren(getV);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.GetV.Builder getVertex = buildGetV(getV);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setVertex(getVertex));
        oprBuilder.addAllMetaData(Utils.physicalProtoRowType(getV.getRowType(), isColumnId));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(getV, oprBuilder.build());
    }

    @Override
    public RelNode visit(GraphLogicalPathExpand pxd) {
        visitChildren(pxd);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.PathExpand.Builder pathExpandBuilder =
                GraphAlgebraPhysical.PathExpand.newBuilder();
        GraphAlgebraPhysical.EdgeExpand.Builder expand =
                buildEdgeExpand((GraphLogicalExpand) pxd.getExpand());
        GraphAlgebraPhysical.GetV.Builder getV = buildGetV((GraphLogicalGetV) pxd.getGetV());
        GraphAlgebraPhysical.PathExpand.ExpandBase.Builder expandBaseBuilder =
                GraphAlgebraPhysical.PathExpand.ExpandBase.newBuilder();
        // TODO: fuse expand and getv
        expandBaseBuilder.setEdgeExpand(expand);
        expandBaseBuilder.setGetV(getV);
        pathExpandBuilder.setBase(expandBaseBuilder);
        pathExpandBuilder.setPathOpt(Utils.protoPathOpt(pxd.getPathOpt()));
        pathExpandBuilder.setResultOpt(Utils.protoPathResultOpt(pxd.getResultOpt()));
        GraphAlgebra.Range range = buildRange(pxd.getOffset(), pxd.getFetch());
        pathExpandBuilder.setHopRange(range);
        pathExpandBuilder.setAlias(Utils.asAliasId(pxd.getAliasId()));
        pathExpandBuilder.setStartTag(Utils.asAliasId(pxd.getStartAlias().getAliasId()));
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setPath(pathExpandBuilder));

        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(pxd, oprBuilder.build());
    }

    @Override
    public RelNode visit(GraphLogicalExpandDegree expandDegree) {
        visitChildren(expandDegree);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.EdgeExpand.Builder edgeExpand =
                buildEdgeExpandDegree(expandDegree.getFusedExpand());
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setEdge(edgeExpand));
        oprBuilder.addAllMetaData(
                Utils.physicalProtoRowType(expandDegree.getRowType(), isColumnId));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(expandDegree, oprBuilder.build());
    }

    @Override
    public RelNode visit(GraphPhysicalExpand physicalExpand) {
        visitChildren(physicalExpand);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.EdgeExpand.Builder edgeExpand = buildEdgeExpandVertex(physicalExpand);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setEdge(edgeExpand));
        oprBuilder.addAllMetaData(
                Utils.physicalProtoRowType(physicalExpand.getRowType(), isColumnId));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(physicalExpand, oprBuilder.build());
    }

    @Override
    public RelNode visit(GraphPhysicalGetV physicalGetV) {
        visitChildren(physicalGetV);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.GetV.Builder auxilia = buildAuxilia(physicalGetV);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setVertex(auxilia));
        oprBuilder.addAllMetaData(
                Utils.physicalProtoRowType(physicalGetV.getRowType(), isColumnId));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(physicalGetV, oprBuilder.build());
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        visitChildren(filter);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebra.Select.Builder selectBuilder = GraphAlgebra.Select.newBuilder();
        OuterExpression.Expression exprProto =
                filter.getCondition()
                        .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
        selectBuilder.setPredicate(exprProto);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setSelect(selectBuilder));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(filter, oprBuilder.build());
    }

    @Override
    public RelNode visit(GraphLogicalProject project) {
        visitChildren(project);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Project.Builder projectBuilder =
                GraphAlgebraPhysical.Project.newBuilder();
        projectBuilder.setIsAppend(project.isAppend());
        List<RelDataTypeField> fields = project.getRowType().getFieldList();
        for (int i = 0; i < project.getProjects().size(); ++i) {
            OuterExpression.Expression expression =
                    project.getProjects()
                            .get(i)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
            int aliasId = fields.get(i).getIndex();
            GraphAlgebraPhysical.Project.ExprAlias.Builder projectExprAliasBuilder =
                    GraphAlgebraPhysical.Project.ExprAlias.newBuilder();
            projectExprAliasBuilder.setExpr(expression);
            projectExprAliasBuilder.setAlias(Utils.asAliasId(aliasId));
            projectBuilder.addMappings(projectExprAliasBuilder.build());
        }
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setProject(projectBuilder));
        oprBuilder.addAllMetaData(Utils.physicalProtoRowType(project.getRowType(), isColumnId));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(project, oprBuilder.build());
    }

    @Override
    public RelNode visit(GraphLogicalAggregate aggregate) {
        visitChildren(aggregate);
        List<RelDataTypeField> fields = aggregate.getRowType().getFieldList();
        List<GraphAggCall> groupCalls = aggregate.getAggCalls();
        GraphGroupKeys keys = aggregate.getGroupKey();
        if (groupCalls.isEmpty()) { // transform to project + dedup by keys
            Preconditions.checkArgument(
                    keys.groupKeyCount() > 0,
                    "group keys should not be empty while group calls is empty");
            GraphAlgebraPhysical.Project.Builder projectBuilder =
                    GraphAlgebraPhysical.Project.newBuilder();
            for (int i = 0; i < keys.groupKeyCount(); ++i) {
                RexNode var = keys.getVariables().get(i);
                Preconditions.checkArgument(
                        var instanceof RexGraphVariable,
                        "each group key should be type %s, but is %s",
                        RexGraphVariable.class,
                        var.getClass());
                OuterExpression.Expression expr =
                        var.accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
                int aliasId;
                if (i >= fields.size()
                        || (aliasId = fields.get(i).getIndex()) == AliasInference.DEFAULT_ID) {
                    throw new IllegalArgumentException(
                            "each group key should have an alias if need dedup");
                }
                GraphAlgebraPhysical.Project.ExprAlias.Builder projectExprAliasBuilder =
                        GraphAlgebraPhysical.Project.ExprAlias.newBuilder();
                projectExprAliasBuilder.setExpr(expr);
                projectExprAliasBuilder.setAlias(Utils.asAliasId(aliasId));
                projectBuilder.addMappings(projectExprAliasBuilder.build());
            }
            GraphAlgebra.Dedup.Builder dedupBuilder = GraphAlgebra.Dedup.newBuilder();
            for (int i = 0; i < keys.groupKeyCount(); ++i) {
                RelDataTypeField field = fields.get(i);
                RexVariable rexVar =
                        RexGraphVariable.of(
                                field.getIndex(),
                                AliasInference.DEFAULT_COLUMN_ID,
                                field.getName(),
                                field.getType());
                OuterExpression.Variable exprVar =
                        rexVar.accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                                .getOperators(0)
                                .getVar();
                dedupBuilder.addKeys(exprVar);
            }
            GraphAlgebraPhysical.PhysicalOpr.Builder projectOprBuilder =
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder();
            projectOprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                            .setProject(projectBuilder));
            GraphAlgebraPhysical.PhysicalOpr.Builder dedupOprBuilder =
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder();
            dedupOprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setDedup(dedupBuilder));
            physicalBuilder.addPlan(projectOprBuilder.build());
            physicalBuilder.addPlan(dedupOprBuilder.build());
            return new PhysicalNode(
                    aggregate,
                    ImmutableList.of(projectOprBuilder.build(), dedupOprBuilder.build()));
        } else {
            GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder();
            GraphAlgebraPhysical.GroupBy.Builder groupByBuilder =
                    GraphAlgebraPhysical.GroupBy.newBuilder();
            for (int i = 0; i < keys.groupKeyCount(); ++i) {
                RexNode var = keys.getVariables().get(i);
                Preconditions.checkArgument(
                        var instanceof RexGraphVariable,
                        "each group key should be type %s, but is %s",
                        RexGraphVariable.class,
                        var.getClass());
                OuterExpression.Variable exprVar =
                        var.accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                                .getOperators(0)
                                .getVar();
                int aliasId = fields.get(i).getIndex();
                GraphAlgebraPhysical.GroupBy.KeyAlias.Builder keyAliasBuilder =
                        GraphAlgebraPhysical.GroupBy.KeyAlias.newBuilder();
                keyAliasBuilder.setKey(exprVar);
                keyAliasBuilder.setAlias(Utils.asAliasId(aliasId));
                groupByBuilder.addMappings(keyAliasBuilder);
            }
            for (int i = 0; i < groupCalls.size(); ++i) {
                List<RexNode> operands = groupCalls.get(i).getOperands();
                if (operands.isEmpty()) {
                    throw new IllegalArgumentException(
                            "operands in aggregate call should not be empty");
                } else if (operands.size() > 1) {
                    throw new UnsupportedOperationException(
                            "aggregate on multiple variables is unsupported yet");
                }
                GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate aggOpt =
                        Utils.protoAggOpt(groupCalls.get(i));
                int aliasId = fields.get(i + keys.groupKeyCount()).getIndex();
                Preconditions.checkArgument(
                        operands.get(0) instanceof RexGraphVariable,
                        "each expression in aggregate call should be type %s, but is %s",
                        RexGraphVariable.class,
                        operands.get(0).getClass());
                OuterExpression.Variable var =
                        operands.get(0)
                                .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                                .getOperators(0)
                                .getVar();
                GraphAlgebraPhysical.GroupBy.AggFunc.Builder aggFnAliasBuilder =
                        GraphAlgebraPhysical.GroupBy.AggFunc.newBuilder();
                aggFnAliasBuilder.setAggregate(aggOpt);
                aggFnAliasBuilder.addVars(var);
                aggFnAliasBuilder.setAlias(Utils.asAliasId(aliasId));
                groupByBuilder.addFunctions(aggFnAliasBuilder);
            }
            oprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                            .setGroupBy(groupByBuilder));
            oprBuilder.addAllMetaData(
                    Utils.physicalProtoRowType(aggregate.getRowType(), isColumnId));
            physicalBuilder.addPlan(oprBuilder.build());
            return new PhysicalNode(aggregate, oprBuilder.build());
        }
    }

    @Override
    public RelNode visit(GraphLogicalSort sort) {
        visitChildren(sort);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        if (!collations.isEmpty()) {
            GraphAlgebra.OrderBy.Builder orderByBuilder = GraphAlgebra.OrderBy.newBuilder();
            for (int i = 0; i < collations.size(); ++i) {
                GraphAlgebra.OrderBy.OrderingPair.Builder orderingPairBuilder =
                        GraphAlgebra.OrderBy.OrderingPair.newBuilder();
                RexGraphVariable expr = ((GraphFieldCollation) collations.get(i)).getVariable();
                OuterExpression.Variable var =
                        expr.accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                                .getOperators(0)
                                .getVar();

                orderingPairBuilder.setKey(var);
                orderingPairBuilder.setOrder(Utils.protoOrderOpt(collations.get(i).getDirection()));
                orderByBuilder.addPairs(orderingPairBuilder.build());
            }
            if (sort.offset != null || sort.fetch != null) {
                orderByBuilder.setLimit(buildRange(sort.offset, sort.fetch));
            }
            oprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                            .setOrderBy(orderByBuilder));
        } else {
            GraphAlgebra.Limit.Builder limitBuilder = GraphAlgebra.Limit.newBuilder();
            limitBuilder.setRange(buildRange(sort.offset, sort.fetch));
            oprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setLimit(limitBuilder));
        }
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(sort, oprBuilder.build());
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Join.Builder joinBuilder = GraphAlgebraPhysical.Join.newBuilder();
        joinBuilder.setJoinKind(Utils.protoJoinKind(join.getJoinType()));
        List<RexNode> conditions = RelOptUtil.conjunctions(join.getCondition());
        String errorMessage =
                "join condition in ir core should be 'AND' of equal conditions, each equal"
                        + " condition has two variables as operands";
        Preconditions.checkArgument(!conditions.isEmpty(), errorMessage);
        for (RexNode condition : conditions) {
            List<RexGraphVariable> leftRightVars = getLeftRightVariables(condition);
            Preconditions.checkArgument(leftRightVars.size() == 2, errorMessage);
            OuterExpression.Variable leftVar =
                    leftRightVars
                            .get(0)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                            .getOperators(0)
                            .getVar();
            OuterExpression.Variable rightVar =
                    leftRightVars
                            .get(1)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                            .getOperators(0)
                            .getVar();
            joinBuilder.addLeftKeys(leftVar);
            joinBuilder.addRightKeys(rightVar);
        }

        GraphAlgebraPhysical.PhysicalPlan.Builder leftPlanBuilder =
                GraphAlgebraPhysical.PhysicalPlan.newBuilder();
        GraphAlgebraPhysical.PhysicalPlan.Builder rightPlanBuilder =
                GraphAlgebraPhysical.PhysicalPlan.newBuilder();

        RelNode left = join.getLeft();
        left.accept(new GraphRelToProtoConverter(isColumnId, graphConfig, leftPlanBuilder));
        RelNode right = join.getRight();
        right.accept(new GraphRelToProtoConverter(isColumnId, graphConfig, rightPlanBuilder));
        joinBuilder.setLeftPlan(leftPlanBuilder);
        joinBuilder.setRightPlan(rightPlanBuilder);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setJoin(joinBuilder));
        physicalBuilder.addPlan(oprBuilder.build());
        return new PhysicalNode(join, oprBuilder.build());
    }

    private List<RexGraphVariable> getLeftRightVariables(RexNode condition) {
        List<RexGraphVariable> vars = Lists.newArrayList();
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator().getKind() == SqlKind.EQUALS) {
                RexNode left = call.getOperands().get(0);
                RexNode right = call.getOperands().get(1);
                if (left instanceof RexGraphVariable && right instanceof RexGraphVariable) {
                    vars.add((RexGraphVariable) left);
                    vars.add((RexGraphVariable) right);
                }
            }
        }
        return vars;
    }

    private GraphAlgebraPhysical.EdgeExpand.Builder buildEdgeExpand(
            GraphLogicalExpand expand, GraphOpt.PhysicalExpandOpt opt) {
        GraphAlgebraPhysical.EdgeExpand.Builder expandBuilder =
                GraphAlgebraPhysical.EdgeExpand.newBuilder();
        expandBuilder.setDirection(Utils.protoExpandOpt(expand.getOpt()));
        expandBuilder.setParams(buildQueryParams(expand));
        expandBuilder.setAlias(Utils.asAliasId(expand.getAliasId()));
        expandBuilder.setVTag(Utils.asAliasId(expand.getStartAlias().getAliasId()));
        expandBuilder.setExpandOpt(Utils.protoPhysicalExpandOpt(opt));
        return expandBuilder;
    }

    private GraphAlgebraPhysical.EdgeExpand.Builder buildEdgeExpand(GraphLogicalExpand expand) {
        return buildEdgeExpand(expand, GraphOpt.PhysicalExpandOpt.EDGE);
    }

    private GraphAlgebraPhysical.EdgeExpand.Builder buildEdgeExpandVertex(
            GraphLogicalExpand expand) {
        return buildEdgeExpand(expand, GraphOpt.PhysicalExpandOpt.VERTEX);
    }

    private GraphAlgebraPhysical.EdgeExpand.Builder buildEdgeExpandDegree(
            GraphLogicalExpand expand) {
        return buildEdgeExpand(expand, GraphOpt.PhysicalExpandOpt.DEGREE);
    }

    private GraphAlgebraPhysical.GetV.Builder buildVertex(
            GraphLogicalGetV getV, GraphOpt.PhysicalGetVOpt opt) {
        GraphAlgebraPhysical.GetV.Builder vertexBuilder = GraphAlgebraPhysical.GetV.newBuilder();
        vertexBuilder.setOpt(Utils.protoPhysicalGetVOpt(opt));
        vertexBuilder.setParams(buildQueryParams(getV));
        vertexBuilder.setAlias(Utils.asAliasId(getV.getAliasId()));
        vertexBuilder.setTag(Utils.asAliasId(getV.getStartAlias().getAliasId()));
        return vertexBuilder;
    }

    private GraphAlgebraPhysical.GetV.Builder buildGetV(GraphLogicalGetV getV) {
        return buildVertex(getV, PhysicalGetVOpt.valueOf(getV.getOpt().name()));
    }

    private GraphAlgebraPhysical.GetV.Builder buildAuxilia(GraphLogicalGetV getV) {
        return buildVertex(getV, PhysicalGetVOpt.ITSELF);
    }

    private GraphAlgebra.Range buildRange(RexNode offset, RexNode fetch) {
        if (offset != null && !(offset instanceof RexLiteral)
                || fetch != null && !(fetch instanceof RexLiteral)) {
            throw new IllegalArgumentException(
                    "can not get INTEGER hops from types instead of RexLiteral");
        }
        GraphAlgebra.Range.Builder rangeBuilder = GraphAlgebra.Range.newBuilder();
        rangeBuilder.setLower(
                offset == null ? 0 : ((Number) ((RexLiteral) offset).getValue()).intValue());
        rangeBuilder.setUpper(
                fetch == null
                        ? Integer.MAX_VALUE
                        : ((Number) ((RexLiteral) fetch).getValue()).intValue());
        return rangeBuilder.build();
    }

    private GraphAlgebra.IndexPredicate buildIndexPredicates(GraphLogicalSource source) {
        RexNode uniqueKeyFilters = source.getUniqueKeyFilters();
        if (uniqueKeyFilters == null) return null;
        // 'within' operator in index predicate is unsupported in ir core, here just
        // expand it to
        // 'or'
        // i.e. '~id within [1, 2]' -> '~id == 1 or ~id == 2'
        RexNode expandSearch = RexUtil.expandSearch(this.rexBuilder, null, uniqueKeyFilters);
        List<RexNode> disjunctions = RelOptUtil.disjunctions(expandSearch);
        // TODO: update index scan in proto, and then build it.
        GraphAlgebra.IndexPredicate.Builder indexBuilder = GraphAlgebra.IndexPredicate.newBuilder();
        return indexBuilder.build();
    }

    private GraphLabelType getGraphLabels(AbstractBindableTableScan tableScan) {
        List<RelDataTypeField> fields = tableScan.getRowType().getFieldList();
        Preconditions.checkArgument(
                !fields.isEmpty() && fields.get(0).getType() instanceof GraphSchemaType,
                "data type of graph operators should be %s ",
                GraphSchemaType.class);
        GraphSchemaType schemaType = (GraphSchemaType) fields.get(0).getType();
        return schemaType.getLabelType();
    }

    private GraphAlgebra.QueryParams buildQueryParams(AbstractBindableTableScan tableScan) {
        Set<Integer> uniqueLabelIds =
                getGraphLabels(tableScan).getLabelsEntry().stream()
                        .map(k -> k.getLabelId())
                        .collect(Collectors.toSet());
        GraphAlgebra.QueryParams.Builder paramsBuilder = GraphAlgebra.QueryParams.newBuilder();
        uniqueLabelIds.forEach(
                k -> {
                    paramsBuilder.addTables(Utils.asNameOrId(k));
                });
        if (ObjectUtils.isNotEmpty(tableScan.getFilters())) {
            OuterExpression.Expression expression =
                    tableScan
                            .getFilters()
                            .get(0)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
            paramsBuilder.setPredicate(expression);
        }
        // TODO: currently no sample rate fused into tableScan, so directly set as 1.0 for tmp.
        paramsBuilder.setSampleRatio(1.0);
        return paramsBuilder.build();
    }

    @Override
    public RelNode visit(GraphLogicalSingleMatch match) {
        throw new UnsupportedOperationException("Unimplemented method 'visit'");
    }

    @Override
    public RelNode visit(GraphLogicalMultiMatch match) {
        throw new UnsupportedOperationException("Unimplemented method 'visit'");
    }
}
