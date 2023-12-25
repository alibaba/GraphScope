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

package com.alibaba.graphscope.common.ir.runtime.ffi;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.ir.rel.*;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rel.type.order.GraphFieldCollation;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToIndexPbConverter;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
import com.alibaba.graphscope.common.ir.runtime.type.PhysicalNode;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.gaia.proto.Common;
import com.alibaba.graphscope.gaia.proto.GraphAlgebra;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.jna.Pointer;

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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * convert a {@code RelNode} to {@code LogicalNode} in ir_core
 */
public class RelToFfiConverter implements GraphRelShuttle {
    private static final Logger logger = LoggerFactory.getLogger(RelToFfiConverter.class);
    private static final IrCoreLibrary LIB = IrCoreLibrary.INSTANCE;
    private final boolean isColumnId;
    private final RexBuilder rexBuilder;

    public RelToFfiConverter(boolean isColumnId, Configs configs) {
        this.isColumnId = isColumnId;
        this.rexBuilder = GraphPlanner.rexBuilderFactory.apply(configs);
    }

    @Override
    public RelNode visit(GraphLogicalSource source) {
        Pointer ptrScan = LIB.initScanOperator(Utils.ffiScanOpt(source.getOpt()));
        if (source.getUniqueKeyFilters() != null) {
            checkFfiResult(
                    LIB.addIndexPredicatePb(
                            ptrScan, ffiIndexPredicates(source.getUniqueKeyFilters())));
        }
        checkFfiResult(LIB.setScanParams(ptrScan, ffiQueryParams(source)));
        if (source.getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(LIB.setScanAlias(ptrScan, ArgUtils.asAlias(source.getAliasId())));
        }
        checkFfiResult(
                LIB.setScanMeta(
                        ptrScan,
                        new FfiPbPointer.ByValue(
                                com.alibaba.graphscope.common.ir.runtime.proto.Utils.protoRowType(
                                                source.getRowType(), isColumnId)
                                        .get(0)
                                        .toByteArray())));
        return new PhysicalNode(source, ptrScan);
    }

    @Override
    public RelNode visit(GraphLogicalExpand expand) {
        Pointer ptrExpand =
                LIB.initEdgexpdOperator(FfiExpandOpt.Edge, Utils.ffiDirection(expand.getOpt()));
        checkFfiResult(LIB.setEdgexpdParams(ptrExpand, ffiQueryParams(expand)));
        if (expand.getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(LIB.setEdgexpdAlias(ptrExpand, ArgUtils.asAlias(expand.getAliasId())));
        }
        if (expand.getStartAlias().getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(
                    LIB.setEdgexpdVtag(
                            ptrExpand, ArgUtils.asNameOrId(expand.getStartAlias().getAliasId())));
        }
        checkFfiResult(
                LIB.setEdgexpdMeta(
                        ptrExpand,
                        new FfiPbPointer.ByValue(
                                com.alibaba.graphscope.common.ir.runtime.proto.Utils.protoRowType(
                                                expand.getRowType(), isColumnId)
                                        .get(0)
                                        .toByteArray())));
        return new PhysicalNode(expand, ptrExpand);
    }

    @Override
    public RelNode visit(GraphLogicalExpandDegree expandCount) {
        GraphLogicalExpand fusedExpand = expandCount.getFusedExpand();
        Pointer ptrExpandCount =
                LIB.initEdgexpdOperator(
                        FfiExpandOpt.Degree, Utils.ffiDirection(fusedExpand.getOpt()));
        checkFfiResult(LIB.setEdgexpdParams(ptrExpandCount, ffiQueryParams(fusedExpand)));
        if (expandCount.getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(
                    LIB.setEdgexpdAlias(
                            ptrExpandCount, ArgUtils.asAlias(expandCount.getAliasId())));
        }
        checkFfiResult(
                LIB.setEdgexpdMeta(
                        ptrExpandCount,
                        new FfiPbPointer.ByValue(
                                com.alibaba.graphscope.common.ir.runtime.proto.Utils.protoRowType(
                                                expandCount.getRowType(), isColumnId)
                                        .get(0)
                                        .toByteArray())));
        return new PhysicalNode(expandCount, ptrExpandCount);
    }

    @Override
    public RelNode visit(GraphLogicalGetV getV) {
        Pointer ptrGetV = LIB.initGetvOperator(Utils.ffiVOpt(getV.getOpt()));
        checkFfiResult(LIB.setGetvParams(ptrGetV, ffiQueryParams(getV)));
        if (getV.getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(LIB.setGetvAlias(ptrGetV, ArgUtils.asAlias(getV.getAliasId())));
        }
        if (getV.getStartAlias().getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(
                    LIB.setGetvTag(
                            ptrGetV, ArgUtils.asNameOrId(getV.getStartAlias().getAliasId())));
        }
        checkFfiResult(
                LIB.setGetvMeta(
                        ptrGetV,
                        new FfiPbPointer.ByValue(
                                com.alibaba.graphscope.common.ir.runtime.proto.Utils.protoRowType(
                                                getV.getRowType(), isColumnId)
                                        .get(0)
                                        .toByteArray())));
        return new PhysicalNode(getV, ptrGetV);
    }

    @Override
    public RelNode visit(GraphLogicalPathExpand pxd) {
        PhysicalNode expand = (PhysicalNode) visit((GraphLogicalExpand) pxd.getExpand());
        PhysicalNode getV = (PhysicalNode) visit((GraphLogicalGetV) pxd.getGetV());
        Pointer ptrPxd =
                LIB.initPathxpdOperatorWithExpandBase(
                        (Pointer) expand.getNode(),
                        (Pointer) getV.getNode(),
                        Utils.ffiPathOpt(pxd.getPathOpt()),
                        Utils.ffiResultOpt(pxd.getResultOpt()));
        List<Integer> hops = range(pxd.getOffset(), pxd.getFetch());
        checkFfiResult(LIB.setPathxpdHops(ptrPxd, hops.get(0), hops.get(1)));
        if (pxd.getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(LIB.setPathxpdAlias(ptrPxd, ArgUtils.asAlias(pxd.getAliasId())));
        }
        if (pxd.getStartAlias().getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(
                    LIB.setPathxpdTag(
                            ptrPxd, ArgUtils.asNameOrId(pxd.getStartAlias().getAliasId())));
        }
        return new PhysicalNode(pxd, ptrPxd);
    }

    @Override
    public RelNode visit(GraphLogicalSingleMatch match) {
        switch (match.getMatchOpt()) {
            case INNER:
                Pointer ptrPattern = LIB.initPatternOperator();
                Pointer ptrSentence = LIB.initPatternSentence(FfiJoinKind.Inner);
                addFfiBinder(ptrSentence, match.getSentence(), true);
                checkFfiResult(LIB.addPatternSentence(ptrPattern, ptrSentence));
                com.alibaba.graphscope.common.ir.runtime.proto.Utils.protoRowType(
                                match.getRowType(), isColumnId)
                        .forEach(
                                k -> {
                                    checkFfiResult(
                                            LIB.addPatternMeta(
                                                    ptrPattern,
                                                    new FfiPbPointer.ByValue(k.toByteArray())));
                                });
                // add dummy source
                Pointer ptrScan = LIB.initScanOperator(Utils.ffiScanOpt(GraphOpt.Source.VERTEX));
                return new PhysicalNode(match, ImmutableList.of(ptrScan, ptrPattern));
            case OPTIONAL:
            case ANTI:
            default:
                throw new UnsupportedOperationException("optional or anti is unsupported yet");
        }
    }

    @Override
    public RelNode visit(GraphLogicalMultiMatch match) {
        Pointer ptrPattern = LIB.initPatternOperator();
        for (RelNode sentence : match.getSentences()) {
            Pointer ptrSentence = LIB.initPatternSentence(FfiJoinKind.Inner);
            addFfiBinder(ptrSentence, sentence, true);
            checkFfiResult(LIB.addPatternSentence(ptrPattern, ptrSentence));
        }
        com.alibaba.graphscope.common.ir.runtime.proto.Utils.protoRowType(
                        match.getRowType(), isColumnId)
                .forEach(
                        k -> {
                            checkFfiResult(
                                    LIB.addPatternMeta(
                                            ptrPattern, new FfiPbPointer.ByValue(k.toByteArray())));
                        });
        // add dummy source
        Pointer ptrScan = LIB.initScanOperator(Utils.ffiScanOpt(GraphOpt.Source.VERTEX));
        return new PhysicalNode(match, ImmutableList.of(ptrScan, ptrPattern));
    }

    @Override
    public RelNode visit(LogicalFilter logicalFilter) {
        OuterExpression.Expression exprProto =
                logicalFilter
                        .getCondition()
                        .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
        Pointer ptrFilter = LIB.initSelectOperator();
        checkFfiResult(
                LIB.setSelectPredicatePb(
                        ptrFilter, new FfiPbPointer.ByValue(exprProto.toByteArray())));
        return new PhysicalNode(logicalFilter, ptrFilter);
    }

    @Override
    public PhysicalNode visit(GraphLogicalProject project) {
        Pointer ptrProject = LIB.initProjectOperator(project.isAppend());
        List<RelDataTypeField> fields = project.getRowType().getFieldList();
        for (int i = 0; i < project.getProjects().size(); ++i) {
            OuterExpression.Expression expression =
                    project.getProjects()
                            .get(i)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
            int aliasId = fields.get(i).getIndex();
            FfiAlias.ByValue ffiAlias =
                    (aliasId == AliasInference.DEFAULT_ID)
                            ? ArgUtils.asNoneAlias()
                            : ArgUtils.asAlias(aliasId);
            checkFfiResult(
                    LIB.addProjectExprPbAlias(
                            ptrProject,
                            new FfiPbPointer.ByValue(expression.toByteArray()),
                            ffiAlias));
        }
        com.alibaba.graphscope.common.ir.runtime.proto.Utils.protoRowType(
                        project.getRowType(), isColumnId)
                .forEach(
                        k -> {
                            checkFfiResult(
                                    LIB.addProjectMeta(
                                            ptrProject, new FfiPbPointer.ByValue(k.toByteArray())));
                        });
        return new PhysicalNode(project, ptrProject);
    }

    @Override
    public PhysicalNode visit(GraphLogicalAggregate aggregate) {
        List<GraphAggCall> groupCalls = aggregate.getAggCalls();
        if (groupCalls.isEmpty()) { // transform to project + dedup by keys
            GraphGroupKeys keys = aggregate.getGroupKey();
            Preconditions.checkArgument(
                    keys.groupKeyCount() > 0,
                    "group keys should not be empty while group calls is empty");
            List<RelDataTypeField> fields = aggregate.getRowType().getFieldList();
            Pointer ptrProject = LIB.initProjectOperator(false);
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
                checkFfiResult(
                        LIB.addProjectExprPbAlias(
                                ptrProject,
                                new FfiPbPointer.ByValue(expr.toByteArray()),
                                ArgUtils.asAlias(aliasId)));
            }
            Pointer ptrDedup = LIB.initDedupOperator();
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
                checkFfiResult(
                        LIB.addDedupKeyPb(
                                ptrDedup, new FfiPbPointer.ByValue(exprVar.toByteArray())));
            }
            return new PhysicalNode(aggregate, ImmutableList.of(ptrProject, ptrDedup));
        }
        Pointer ptrGroup = LIB.initGroupbyOperator();
        List<RelDataTypeField> fields = aggregate.getRowType().getFieldList();
        List<RexNode> groupKeys = aggregate.getGroupKey().getVariables();
        // if groupKeys is empty -> calculate the global aggregated values, i.e. g.V().count()
        for (int i = 0; i < groupKeys.size(); ++i) {
            Preconditions.checkArgument(
                    groupKeys.get(i) instanceof RexGraphVariable,
                    "each group key should be type %s, but is %s",
                    RexGraphVariable.class,
                    groupKeys.get(i).getClass());
            OuterExpression.Variable var =
                    groupKeys
                            .get(i)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                            .getOperators(0)
                            .getVar();
            int aliasId = fields.get(i).getIndex();
            FfiAlias.ByValue ffiAlias =
                    (aliasId == AliasInference.DEFAULT_ID)
                            ? ArgUtils.asNoneAlias()
                            : ArgUtils.asAlias(aliasId);
            checkFfiResult(
                    LIB.addGroupbyKeyPbAlias(
                            ptrGroup, new FfiPbPointer.ByValue(var.toByteArray()), ffiAlias));
        }
        for (int i = 0; i < groupCalls.size(); ++i) {
            List<RexNode> operands = groupCalls.get(i).getOperands();
            if (operands.isEmpty()) {
                throw new IllegalArgumentException(
                        "operands in aggregate call should not be empty");
            }
            int aliasId = fields.get(i + groupKeys.size()).getIndex();
            Common.NameOrId alias =
                    (aliasId == AliasInference.DEFAULT_ID)
                            ? Common.NameOrId.newBuilder().build()
                            : Common.NameOrId.newBuilder().setId(aliasId).build();
            List<OuterExpression.Variable> vars =
                    operands.stream()
                            .map(
                                    k -> {
                                        Preconditions.checkArgument(
                                                k instanceof RexGraphVariable,
                                                "each operand in aggregate call should be type %s,"
                                                        + " but is %s",
                                                RexGraphVariable.class,
                                                k.getClass());
                                        return k.accept(
                                                        new RexToProtoConverter(
                                                                true, isColumnId, this.rexBuilder))
                                                .getOperators(0)
                                                .getVar();
                                    })
                            .collect(Collectors.toList());
            checkFfiResult(
                    LIB.addGroupbyAggFnPb(
                            ptrGroup,
                            new FfiPbPointer.ByValue(
                                    GraphAlgebra.GroupBy.AggFunc.newBuilder()
                                            .addAllVars(vars)
                                            .setAggregate(
                                                    com.alibaba.graphscope.common.ir.runtime.proto
                                                            .Utils.protoAggFn(groupCalls.get(i)))
                                            .setAlias(alias)
                                            .build()
                                            .toByteArray())));
        }
        com.alibaba.graphscope.common.ir.runtime.proto.Utils.protoRowType(
                        aggregate.getRowType(), isColumnId)
                .forEach(
                        k -> {
                            checkFfiResult(
                                    LIB.addGroupbyKeyValueMeta(
                                            ptrGroup, new FfiPbPointer.ByValue(k.toByteArray())));
                        });
        return new PhysicalNode(aggregate, ptrGroup);
    }

    @Override
    public PhysicalNode visit(GraphLogicalDedupBy dedupBy) {
        Preconditions.checkArgument(
                !dedupBy.getDedupByKeys().isEmpty(), "dedup by keys should not be empty");
        Pointer ptrDedup = LIB.initDedupOperator();
        for (RexNode key : dedupBy.getDedupByKeys()) {
            Preconditions.checkArgument(
                    key instanceof RexGraphVariable,
                    "each dedup by key should be type %s, but is %s",
                    RexGraphVariable.class,
                    key.getClass());
            OuterExpression.Variable var =
                    key.accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                            .getOperators(0)
                            .getVar();
            checkFfiResult(
                    LIB.addDedupKeyPb(ptrDedup, new FfiPbPointer.ByValue(var.toByteArray())));
        }
        return new PhysicalNode(dedupBy, ptrDedup);
    }

    @Override
    public PhysicalNode visit(GraphLogicalSort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        Pointer ptrNode = null;
        if (!collations.isEmpty()) {
            ptrNode = LIB.initOrderbyOperator();
            for (int i = 0; i < collations.size(); ++i) {
                RexGraphVariable expr = ((GraphFieldCollation) collations.get(i)).getVariable();
                OuterExpression.Variable var =
                        expr.accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                                .getOperators(0)
                                .getVar();
                checkFfiResult(
                        LIB.addOrderbyPairPb(
                                ptrNode,
                                new FfiPbPointer.ByValue(var.toByteArray()),
                                Utils.ffiOrderOpt(collations.get(i).direction)));
            }
        }
        if (sort.offset != null || sort.fetch != null) {
            List<Integer> limitRange = range(sort.offset, sort.fetch);
            if (collations.isEmpty()) {
                ptrNode = LIB.initLimitOperator();
                checkFfiResult(LIB.setLimitRange(ptrNode, limitRange.get(0), limitRange.get(1)));
            } else if (ptrNode != null) {
                checkFfiResult(LIB.setOrderbyLimit(ptrNode, limitRange.get(0), limitRange.get(1)));
            }
        }
        return new PhysicalNode(sort, ptrNode);
    }

    @Override
    public PhysicalNode visit(LogicalJoin join) {
        Pointer ptrJoin = LIB.initJoinOperator(Utils.ffiJoinKind(join.getJoinType()));
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
            checkFfiResult(
                    LIB.addJoinKeyPairPb(
                            ptrJoin,
                            new FfiPbPointer.ByValue(leftVar.toByteArray()),
                            new FfiPbPointer.ByValue(rightVar.toByteArray())));
        }
        return new PhysicalNode(join, ptrJoin);
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

    private Pointer ffiQueryParams(AbstractBindableTableScan tableScan) {
        Set<Integer> uniqueLabelIds =
                getGraphLabels(tableScan).getLabelsEntry().stream()
                        .map(k -> k.getLabelId())
                        .collect(Collectors.toSet());
        Pointer params = LIB.initQueryParams();
        uniqueLabelIds.forEach(
                k -> {
                    checkFfiResult(LIB.addParamsTable(params, ArgUtils.asNameOrId(k)));
                });
        if (ObjectUtils.isNotEmpty(tableScan.getFilters())) {
            OuterExpression.Expression expression =
                    tableScan
                            .getFilters()
                            .get(0)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
            checkFfiResult(
                    LIB.setParamsPredicatePb(
                            params, new FfiPbPointer.ByValue(expression.toByteArray())));
        }
        return params;
    }

    private FfiPbPointer.ByValue ffiIndexPredicates(RexNode uniqueKeyFilters) {
        GraphAlgebra.IndexPredicate indexPredicate =
                uniqueKeyFilters.accept(
                        new RexToIndexPbConverter(true, this.isColumnId, this.rexBuilder));
        return new FfiPbPointer.ByValue(indexPredicate.toByteArray());
    }

    private List<Integer> range(RexNode offset, RexNode fetch) {
        // offset or fetch can be a dynamic params to support prepared statement
        if (offset != null && !(offset instanceof RexLiteral)
                || fetch != null && !(fetch instanceof RexLiteral)) {
            throw new IllegalArgumentException(
                    "can not get INTEGER hops from types instead of RexLiteral");
        }
        int lower = (offset == null) ? 0 : ((Number) ((RexLiteral) offset).getValue()).intValue();
        int upper =
                (fetch == null)
                        ? Integer.MAX_VALUE
                        : lower + ((Number) ((RexLiteral) fetch).getValue()).intValue();
        return ImmutableList.of(lower, upper);
    }

    private void addFfiBinder(Pointer ptrSentence, RelNode binder, boolean isTail) {
        if (binder.getInputs().isEmpty()) { // start operator
            // should start from all vertices
            if (!(binder instanceof GraphLogicalSource)
                    || ((GraphLogicalSource) binder).getOpt() != GraphOpt.Source.VERTEX) {
                throw new IllegalArgumentException(
                        "start binder in each sentence should be vertex type");
            }
            GraphLogicalSource source = (GraphLogicalSource) binder;
            int aliasId = source.getAliasId();
            // should have set a start tag
            if (aliasId == AliasInference.DEFAULT_ID) {
                throw new IllegalArgumentException("start binder should have a given tag");
            }
            checkFfiResult(LIB.setSentenceStart(ptrSentence, ArgUtils.asNameOrId(aliasId)));
            addFilterToFfiBinder(ptrSentence, source);
        } else {
            addFfiBinder(ptrSentence, binder.getInput(0), false);
            if (binder instanceof AbstractBindableTableScan) {
                if (binder instanceof GraphLogicalExpand) {
                    PhysicalNode node = (PhysicalNode) visit((GraphLogicalExpand) binder);
                    checkFfiResult(
                            LIB.addSentenceBinder(
                                    ptrSentence, (Pointer) node.getNode(), FfiBinderOpt.Edge));
                } else { // getV
                    PhysicalNode node = (PhysicalNode) visit((GraphLogicalGetV) binder);
                    checkFfiResult(
                            LIB.addSentenceBinder(
                                    ptrSentence, (Pointer) node.getNode(), FfiBinderOpt.Vertex));
                }
                int aliasId = ((AbstractBindableTableScan) binder).getAliasId();
                if (isTail && (aliasId != AliasInference.DEFAULT_ID)) { // end operator
                    checkFfiResult(LIB.setSentenceEnd(ptrSentence, ArgUtils.asNameOrId(aliasId)));
                }
            } else if (binder instanceof GraphLogicalPathExpand) { // path expand
                PhysicalNode node = (PhysicalNode) visit((GraphLogicalPathExpand) binder);
                checkFfiResult(
                        LIB.addSentenceBinder(
                                ptrSentence, (Pointer) node.getNode(), FfiBinderOpt.Path));
                int aliasId = ((GraphLogicalPathExpand) binder).getAliasId();
                if (isTail && (aliasId != AliasInference.DEFAULT_ID)) { // end operator
                    checkFfiResult(LIB.setSentenceEnd(ptrSentence, ArgUtils.asNameOrId(aliasId)));
                }
            } else {
                throw new IllegalArgumentException(
                        "invalid type " + binder.getClass() + " in match sentence");
            }
        }
    }

    private void addFilterToFfiBinder(Pointer ptrSentence, AbstractBindableTableScan tableScan) {
        Set<Integer> uniqueLabelIds =
                getGraphLabels(tableScan).getLabelsEntry().stream()
                        .map(k -> k.getLabelId())
                        .collect(Collectors.toSet());
        // add labels as select operator
        if (ObjectUtils.isNotEmpty(uniqueLabelIds)) {
            Pointer ptrFilter = LIB.initSelectOperator();
            StringBuilder predicateBuilder = new StringBuilder();
            predicateBuilder.append("@.~label within [");
            int i = 0;
            for (Iterator<Integer> it = uniqueLabelIds.iterator(); it.hasNext(); ++i) {
                if (i > 0) {
                    predicateBuilder.append(",");
                }
                predicateBuilder.append(it.next());
            }
            predicateBuilder.append("]");
            checkFfiResult(LIB.setSelectPredicate(ptrFilter, predicateBuilder.toString()));
            checkFfiResult(LIB.addSentenceBinder(ptrSentence, ptrFilter, FfiBinderOpt.Select));
        }
        // add predicates as select operator
        List<RexNode> filters = tableScan.getFilters();
        if (ObjectUtils.isNotEmpty(filters)) {
            OuterExpression.Expression exprProto =
                    filters.get(0)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
            Pointer ptrFilter = LIB.initSelectOperator();
            checkFfiResult(
                    LIB.setSelectPredicatePb(
                            ptrFilter, new FfiPbPointer.ByValue(exprProto.toByteArray())));
            checkFfiResult(LIB.addSentenceBinder(ptrSentence, ptrFilter, FfiBinderOpt.Select));
        }
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

    private void checkFfiResult(FfiResult res) {
        if (res == null || res.code != ResultCode.Success) {
            throw new IllegalStateException(
                    "build logical node, unexpected ffi results from ir_core, msg : %s" + res);
        }
    }
}
