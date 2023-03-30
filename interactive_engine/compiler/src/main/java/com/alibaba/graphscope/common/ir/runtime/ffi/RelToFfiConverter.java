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

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalAggregate;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalProject;
import com.alibaba.graphscope.common.ir.rel.GraphLogicalSort;
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttle;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.order.GraphFieldCollation;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.runtime.proto.RexToProtoConverter;
import com.alibaba.graphscope.common.ir.runtime.type.LogicalNode;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphProperty;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;
import com.alibaba.graphscope.common.ir.type.GraphSchemaTypeList;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.sun.jna.Pointer;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

    public RelToFfiConverter(boolean isColumnId) {
        this.isColumnId = isColumnId;
    }

    @Override
    public RelNode visit(GraphLogicalSource source) {
        Pointer ptrScan = LIB.initScanOperator(Utils.ffiScanOpt(source.getOpt()));
        Pointer ptrIndex = ffiIndexPredicates(source);
        if (ptrIndex != null) {
            checkFfiResult(LIB.addScanIndexPredicate(ptrScan, ptrIndex));
        }
        checkFfiResult(LIB.setScanParams(ptrScan, ffiQueryParams(source)));
        if (source.getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(LIB.setScanAlias(ptrScan, ArgUtils.asAlias(source.getAliasId())));
        }
        return new LogicalNode(source, ptrScan);
    }

    @Override
    public RelNode visit(GraphLogicalExpand expand) {
        Pointer ptrExpand =
                LIB.initEdgexpdOperator(FfiExpandOpt.Edge, Utils.ffiDirection(expand.getOpt()));
        checkFfiResult(LIB.setEdgexpdParams(ptrExpand, ffiQueryParams(expand)));
        if (expand.getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(LIB.setEdgexpdAlias(ptrExpand, ArgUtils.asAlias(expand.getAliasId())));
        }
        return new LogicalNode(expand, ptrExpand);
    }

    @Override
    public RelNode visit(GraphLogicalGetV getV) {
        Pointer ptrGetV = LIB.initGetvOperator(Utils.ffiVOpt(getV.getOpt()));
        checkFfiResult(LIB.setGetvParams(ptrGetV, ffiQueryParams(getV)));
        if (getV.getAliasId() != AliasInference.DEFAULT_ID) {
            checkFfiResult(LIB.setGetvAlias(ptrGetV, ArgUtils.asAlias(getV.getAliasId())));
        }
        return new LogicalNode(getV, ptrGetV);
    }

    @Override
    public RelNode visit(GraphLogicalPathExpand pxd) {
        LogicalNode expand = (LogicalNode) visit((GraphLogicalExpand) pxd.getExpand());
        LogicalNode getV = (LogicalNode) visit((GraphLogicalGetV) pxd.getGetV());
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
        return new LogicalNode(pxd, ptrPxd);
    }

    @Override
    public RelNode visit(GraphLogicalSingleMatch match) {
        switch (match.getMatchOpt()) {
            case INNER:
                Pointer ptrPattern = LIB.initPatternOperator();
                Pointer ptrSentence = LIB.initPatternSentence(FfiJoinKind.Inner);
                addFfiBinder(ptrSentence, match.getSentence(), true);
                checkFfiResult(LIB.addPatternSentence(ptrPattern, ptrSentence));
                return new LogicalNode(match, ptrPattern);
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
        return new LogicalNode(match, ptrPattern);
    }

    @Override
    public RelNode visit(LogicalFilter logicalFilter) {
        OuterExpression.Expression exprProto =
                logicalFilter.getCondition().accept(new RexToProtoConverter(true, isColumnId));
        Pointer ptrFilter = LIB.initSelectOperator();
        checkFfiResult(
                LIB.setSelectPredicatePb(
                        ptrFilter, new FfiPbPointer.ByValue(exprProto.toByteArray())));
        return new LogicalNode(logicalFilter, ptrFilter);
    }

    @Override
    public LogicalNode visit(GraphLogicalProject project) {
        Pointer ptrProject = LIB.initProjectOperator(project.isAppend());
        List<RelDataTypeField> fields = project.getRowType().getFieldList();
        for (int i = 0; i < project.getProjects().size(); ++i) {
            OuterExpression.Expression expression =
                    project.getProjects().get(i).accept(new RexToProtoConverter(true, isColumnId));
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
        return new LogicalNode(project, ptrProject);
    }

    @Override
    public LogicalNode visit(GraphLogicalAggregate aggregate) {
        List<GraphAggCall> groupCalls = aggregate.getAggCalls();
        if (groupCalls.isEmpty()) { // transform to project + dedup by keys
            return new LogicalNode(aggregate, null);
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
                            .accept(new RexToProtoConverter(true, isColumnId))
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
            } else if (operands.size() > 1) {
                throw new UnsupportedOperationException(
                        "aggregate on multiple variables is unsupported yet");
            }
            FfiAggOpt ffiAggOpt = Utils.ffiAggOpt(groupCalls.get(i));
            int aliasId = fields.get(i + groupKeys.size()).getIndex();
            FfiAlias.ByValue ffiAlias =
                    (aliasId == AliasInference.DEFAULT_ID)
                            ? ArgUtils.asNoneAlias()
                            : ArgUtils.asAlias(aliasId);
            Preconditions.checkArgument(
                    operands.get(0) instanceof RexGraphVariable,
                    "each expression in aggregate call should be type %s, but is %s",
                    RexGraphVariable.class,
                    operands.get(0).getClass());
            OuterExpression.Variable var =
                    operands.get(0)
                            .accept(new RexToProtoConverter(true, isColumnId))
                            .getOperators(0)
                            .getVar();
            checkFfiResult(
                    LIB.addGroupbyAggFnPb(
                            ptrGroup,
                            new FfiPbPointer.ByValue(var.toByteArray()),
                            ffiAggOpt,
                            ffiAlias));
        }
        return new LogicalNode(aggregate, ptrGroup);
    }

    @Override
    public LogicalNode visit(GraphLogicalSort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        Pointer ptrNode = null;
        if (!collations.isEmpty()) {
            ptrNode = LIB.initOrderbyOperator();
            for (int i = 0; i < collations.size(); ++i) {
                RexGraphVariable expr = ((GraphFieldCollation) collations.get(i)).getVariable();
                OuterExpression.Variable var =
                        expr.accept(new RexToProtoConverter(true, isColumnId))
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
        return new LogicalNode(sort, ptrNode);
    }

    private Pointer ffiQueryParams(AbstractBindableTableScan tableScan) {
        Set<Integer> uniqueLabelIds =
                getGraphLabels(tableScan).stream()
                        .map(k -> k.getLabelId())
                        .collect(Collectors.toSet());
        Pointer params = LIB.initQueryParams();
        uniqueLabelIds.forEach(
                k -> {
                    checkFfiResult(LIB.addParamsTable(params, ArgUtils.asNameOrId(k)));
                });
        if (ObjectUtils.isNotEmpty(tableScan.getFilters())) {
            OuterExpression.Expression expression =
                    tableScan.getFilters().get(0).accept(new RexToProtoConverter(true, isColumnId));
            checkFfiResult(
                    LIB.setParamsPredicatePb(
                            params, new FfiPbPointer.ByValue(expression.toByteArray())));
        }
        return params;
    }

    private @Nullable Pointer ffiIndexPredicates(GraphLogicalSource source) {
        ImmutableList<RexNode> filters = source.getFilters();
        if (ObjectUtils.isEmpty(filters)) return null;
        // decomposed by OR
        List<RexNode> disJunctions = RelOptUtil.disjunctions(filters.get(0));
        List<RexNode> literals = new ArrayList<>();
        for (RexNode rexNode : disJunctions) {
            if (!isIdEqualsLiteral(rexNode, literals)) {
                return null;
            }
        }
        Pointer ptrIndex = LIB.initIndexPredicate();
        for (RexNode literal : literals) {
            FfiProperty.ByValue property = new FfiProperty.ByValue();
            property.opt = FfiPropertyOpt.Id;
            checkFfiResult(
                    LIB.orEquivPredicate(ptrIndex, property, Utils.ffiConst((RexLiteral) literal)));
        }
        // remove index predicates from filter conditions
        source.setFilters(ImmutableList.of());
        return ptrIndex;
    }

    // i.e. a.~id == 10
    private boolean isIdEqualsLiteral(RexNode rexNode, List<RexNode> literal) {
        if (rexNode.getKind() != SqlKind.EQUALS) return false;
        List<RexNode> operands = ((RexCall) rexNode).getOperands();
        return isGlobalId(operands.get(0)) && isLiteral(operands.get(1), literal)
                || isGlobalId(operands.get(1)) && isLiteral(operands.get(0), literal);
    }

    // i.e. a.~id
    private boolean isGlobalId(RexNode rexNode) {
        return rexNode instanceof RexGraphVariable
                && ((RexGraphVariable) rexNode).getProperty().getOpt() == GraphProperty.Opt.ID;
    }

    private boolean isLiteral(RexNode rexNode, List<RexNode> literal) {
        boolean isLiteral = rexNode.getKind() == SqlKind.LITERAL;
        if (isLiteral) literal.add(rexNode);
        return isLiteral;
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
                    LogicalNode node = (LogicalNode) visit((GraphLogicalExpand) binder);
                    checkFfiResult(
                            LIB.addSentenceBinder(
                                    ptrSentence, (Pointer) node.getNode(), FfiBinderOpt.Edge));
                } else { // getV
                    LogicalNode node = (LogicalNode) visit((GraphLogicalGetV) binder);
                    checkFfiResult(
                            LIB.addSentenceBinder(
                                    ptrSentence, (Pointer) node.getNode(), FfiBinderOpt.Vertex));
                }
                int aliasId = ((AbstractBindableTableScan) binder).getAliasId();
                if (isTail && (aliasId != AliasInference.DEFAULT_ID)) { // end operator
                    checkFfiResult(LIB.setSentenceEnd(ptrSentence, ArgUtils.asNameOrId(aliasId)));
                }
            } else if (binder instanceof GraphLogicalPathExpand) { // path expand
                LogicalNode node = (LogicalNode) visit((GraphLogicalPathExpand) binder);
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
                getGraphLabels(tableScan).stream()
                        .map(k -> k.getLabelId())
                        .collect(Collectors.toSet());
        // add labels as select operator
        if (ObjectUtils.isNotEmpty(uniqueLabelIds)) {
            Pointer ptrFilter = LIB.initSelectOperator();
            StringBuilder predicateBuilder = new StringBuilder();
            int i = 0;
            for (Iterator<Integer> it = uniqueLabelIds.iterator(); it.hasNext(); ++i) {
                if (i > 0) {
                    predicateBuilder.append(" || ");
                }
                predicateBuilder.append("@.~label == " + it.next());
            }
            checkFfiResult(LIB.setSelectPredicate(ptrFilter, predicateBuilder.toString()));
            checkFfiResult(LIB.addSentenceBinder(ptrSentence, ptrFilter, FfiBinderOpt.Select));
        }
        // add predicates as select operator
        List<RexNode> filters = tableScan.getFilters();
        if (ObjectUtils.isNotEmpty(filters)) {
            OuterExpression.Expression exprProto =
                    filters.get(0).accept(new RexToProtoConverter(true, isColumnId));
            Pointer ptrFilter = LIB.initSelectOperator();
            checkFfiResult(
                    LIB.setSelectPredicatePb(
                            ptrFilter, new FfiPbPointer.ByValue(exprProto.toByteArray())));
            checkFfiResult(LIB.addSentenceBinder(ptrSentence, ptrFilter, FfiBinderOpt.Select));
        }
    }

    private List<GraphLabelType> getGraphLabels(AbstractBindableTableScan tableScan) {
        List<RelDataTypeField> fields = tableScan.getRowType().getFieldList();
        Preconditions.checkArgument(
                !fields.isEmpty() && fields.get(0).getType() instanceof GraphSchemaType,
                "data type of graph operators should be %s ",
                GraphSchemaType.class);
        GraphSchemaType schemaType = (GraphSchemaType) fields.get(0).getType();
        List<GraphLabelType> labelTypes = new ArrayList<>();
        if (schemaType instanceof GraphSchemaTypeList) {
            ((GraphSchemaTypeList) schemaType).forEach(k -> labelTypes.add(k.getLabelType()));
        } else {
            labelTypes.add(schemaType.getLabelType());
        }
        return labelTypes;
    }

    private void checkFfiResult(FfiResult res) {
        if (res == null || res.code != ResultCode.Success) {
            throw new IllegalStateException(
                    "build logical node, unexpected ffi results from ir_core, msg : %s" + res);
        }
    }
}
