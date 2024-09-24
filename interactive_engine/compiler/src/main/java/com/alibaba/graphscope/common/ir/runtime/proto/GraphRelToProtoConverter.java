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
import com.alibaba.graphscope.common.config.PegasusConfig;
import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.*;
import com.alibaba.graphscope.common.ir.rel.graph.*;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphAggCall;
import com.alibaba.graphscope.common.ir.rel.type.group.GraphGroupKeys;
import com.alibaba.graphscope.common.ir.rel.type.order.GraphFieldCollation;
import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt.PhysicalGetVOpt;
import com.alibaba.graphscope.common.ir.type.GraphLabelType;
import com.alibaba.graphscope.common.ir.type.GraphNameOrId;
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
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GraphRelToProtoConverter extends GraphShuttle {
    private final boolean isColumnId;
    private final RexBuilder rexBuilder;
    private final Configs graphConfig;
    private GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder;
    private final boolean isPartitioned;
    private boolean preCacheEdgeProps;
    private final IdentityHashMap<RelNode, List<CommonTableScan>> relToCommons;
    private final int depth;
    private final HashMap<String, String> extraParams = new HashMap<>();

    public GraphRelToProtoConverter(
            boolean isColumnId,
            Configs configs,
            GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder,
            IdentityHashMap<RelNode, List<CommonTableScan>> relToCommons,
            HashMap<String, String> extraParams) {
        this(isColumnId, configs, physicalBuilder, relToCommons, extraParams, 0);
    }

    public GraphRelToProtoConverter(
            boolean isColumnId,
            Configs configs,
            GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder,
            IdentityHashMap<RelNode, List<CommonTableScan>> relToCommons,
            HashMap<String, String> extraParams,
            int depth) {
        this.isColumnId = isColumnId;
        this.rexBuilder = GraphPlanner.rexBuilderFactory.apply(configs);
        this.graphConfig = configs;
        this.physicalBuilder = physicalBuilder;
        this.isPartitioned =
                !(PegasusConfig.PEGASUS_HOSTS.get(configs).split(",").length == 1
                        && PegasusConfig.PEGASUS_WORKER_NUM.get(configs) == 1);
        // currently, since the store doesn't support get properties from edges, we always need to
        // precache edge properties.
        this.preCacheEdgeProps = true;
        this.relToCommons = relToCommons;
        this.extraParams.putAll(extraParams);
        this.depth = depth;
    }

    @Override
    public RelNode visit(GraphProcedureCall procedureCall) {
        visitChildren(procedureCall);
        physicalBuilder.addPlan(
                GraphAlgebraPhysical.PhysicalOpr.newBuilder()
                        .setOpr(
                                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                                        .setProcedureCall(
                                                GraphAlgebraPhysical.ProcedureCall.newBuilder()
                                                        .setQuery(
                                                                Utils.protoProcedure(
                                                                        procedureCall
                                                                                .getProcedure(),
                                                                        new RexToProtoConverter(
                                                                                true,
                                                                                isColumnId,
                                                                                this.rexBuilder))))
                                        .build())
                        .addAllMetaData(
                                Utils.physicalProtoRowType(procedureCall.getRowType(), isColumnId))
                        .build());
        return procedureCall;
    }

    @Override
    public RelNode visit(GraphLogicalSource source) {
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Scan.Builder scanBuilder = GraphAlgebraPhysical.Scan.newBuilder();
        RexNode uniqueKeyFilters = source.getUniqueKeyFilters();
        if (uniqueKeyFilters != null) {
            GraphAlgebra.IndexPredicate indexPredicate = buildIndexPredicates(uniqueKeyFilters);
            scanBuilder.setIdxPredicate(indexPredicate);
        }
        GraphAlgebra.QueryParams.Builder queryParamsBuilder = buildQueryParams(source);
        if (preCacheEdgeProps && GraphOpt.Source.EDGE.equals(source.getOpt())) {
            addQueryColumns(
                    queryParamsBuilder,
                    Utils.extractColumnsFromRelDataType(source.getRowType(), isColumnId));
        }
        scanBuilder.setParams(queryParamsBuilder);
        if (source.getAliasId() != AliasInference.DEFAULT_ID) {
            scanBuilder.setAlias(Utils.asAliasId(source.getAliasId()));
        }
        scanBuilder.setScanOpt(Utils.protoScanOpt(source.getOpt()));
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setScan(scanBuilder));
        oprBuilder.addAllMetaData(Utils.physicalProtoRowType(source.getRowType(), isColumnId));
        physicalBuilder.addPlan(oprBuilder.build());
        return source;
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
        if (isPartitioned) {
            addRepartitionToAnother(expand.getStartAlias().getAliasId());
        }
        physicalBuilder.addPlan(oprBuilder.build());
        return expand;
    }

    @Override
    public RelNode visit(GraphLogicalGetV getV) {
        visitChildren(getV);
        // convert getV:
        // if there is no filter, build getV(adj)
        // otherwise, build getV(adj) + auxilia(filter)
        if (ObjectUtils.isEmpty(getV.getFilters())) {
            GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder();
            GraphAlgebraPhysical.GetV.Builder getVertex = buildGetV(getV);
            oprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setVertex(getVertex));
            oprBuilder.addAllMetaData(Utils.physicalProtoRowType(getV.getRowType(), isColumnId));
            physicalBuilder.addPlan(oprBuilder.build());
            return getV;
        } else {
            // build getV(adj) + auxilia(filter) if there is a filter in getV
            GraphAlgebraPhysical.PhysicalOpr.Builder adjOprBuilder =
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder();
            GraphAlgebraPhysical.GetV.Builder adjVertexBuilder =
                    GraphAlgebraPhysical.GetV.newBuilder();
            adjVertexBuilder.setOpt(
                    Utils.protoGetVOpt(PhysicalGetVOpt.valueOf(getV.getOpt().name())));
            // 1. build adjV without filter
            GraphAlgebra.QueryParams.Builder adjParamsBuilder = defaultQueryParams();
            addQueryTables(
                    adjParamsBuilder,
                    com.alibaba.graphscope.common.ir.tools.Utils.getGraphLabels(getV.getRowType())
                            .getLabelsEntry());
            adjVertexBuilder.setParams(adjParamsBuilder);
            if (getV.getStartAlias().getAliasId() != AliasInference.DEFAULT_ID) {
                adjVertexBuilder.setTag(Utils.asAliasId(getV.getStartAlias().getAliasId()));
            }
            if (getV.getAliasId() != AliasInference.DEFAULT_ID) {
                adjVertexBuilder.setAlias(Utils.asAliasId(getV.getAliasId()));
            }
            adjOprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                            .setVertex(adjVertexBuilder));
            adjOprBuilder.addAllMetaData(Utils.physicalProtoRowType(getV.getRowType(), isColumnId));
            physicalBuilder.addPlan(adjOprBuilder.build());

            // 2. build auxilia(filter)
            GraphAlgebraPhysical.PhysicalOpr.Builder auxiliaOprBuilder =
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder();
            GraphAlgebraPhysical.GetV.Builder auxiliaBuilder =
                    GraphAlgebraPhysical.GetV.newBuilder();
            auxiliaBuilder.setOpt(Utils.protoGetVOpt(PhysicalGetVOpt.ITSELF));
            GraphAlgebra.QueryParams.Builder auxiliaParamsBuilder = defaultQueryParams();
            addQueryFilters(auxiliaParamsBuilder, getV.getFilters());
            auxiliaBuilder.setParams(auxiliaParamsBuilder);
            auxiliaOprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                            .setVertex(auxiliaBuilder));
            auxiliaOprBuilder.addAllMetaData(
                    Utils.physicalProtoRowType(getV.getRowType(), isColumnId));
            if (isPartitioned) {
                addRepartitionToAnother(getV.getAliasId());
            }
            physicalBuilder.addPlan(auxiliaOprBuilder.build());
            return getV;
        }
    }

    @Override
    public RelNode visit(GraphLogicalPathExpand pxd) {
        visitChildren(pxd);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.PathExpand.Builder pathExpandBuilder =
                GraphAlgebraPhysical.PathExpand.newBuilder();
        GraphAlgebraPhysical.PathExpand.ExpandBase.Builder expandBaseBuilder =
                GraphAlgebraPhysical.PathExpand.ExpandBase.newBuilder();
        RelNode fused = pxd.getFused();
        GraphAlgebraPhysical.EdgeExpand.Builder edgeExpandBuilder;
        GraphAlgebraPhysical.GetV.Builder getVBuilder = null;
        // preserve the original fused logical getv to cache properties if necessary
        GraphLogicalGetV originalGetV;
        RelDataType rowType;
        if (fused != null) {
            // the case that expand base is fused
            if (fused instanceof GraphPhysicalGetV) {
                // fused into expand + auxilia
                GraphPhysicalGetV fusedGetV = (GraphPhysicalGetV) fused;
                getVBuilder = buildAuxilia(fusedGetV);
                originalGetV = fusedGetV.getFusedGetV();
                if (fusedGetV.getInput() instanceof GraphPhysicalExpand) {
                    GraphPhysicalExpand fusedExpand = (GraphPhysicalExpand) fusedGetV.getInput();
                    edgeExpandBuilder = buildEdgeExpand(fusedExpand);
                    rowType = fusedExpand.getRowType();
                } else {
                    throw new UnsupportedOperationException(
                            "unsupported fused plan in path expand base: "
                                    + fusedGetV.getInput().getClass().getName());
                }
            } else if (fused instanceof GraphPhysicalExpand) {
                // fused into expand
                GraphPhysicalExpand fusedExpand = (GraphPhysicalExpand) fused;
                edgeExpandBuilder = buildEdgeExpand(fusedExpand);
                originalGetV = fusedExpand.getFusedGetV();
                rowType = fusedExpand.getFusedExpand().getRowType();
            } else {
                throw new UnsupportedOperationException(
                        "unsupported fused plan in path expand base");
            }
        } else {
            // the case that expand base is not fused
            edgeExpandBuilder = buildEdgeExpand((GraphLogicalExpand) pxd.getExpand());
            originalGetV = (GraphLogicalGetV) pxd.getGetV();
            getVBuilder = buildGetV(originalGetV);
            rowType = pxd.getExpand().getRowType();
        }
        expandBaseBuilder.setEdgeExpand(edgeExpandBuilder);
        // specifically, for path expand (except that it has PathExpandResult as END_V), we need to
        // cache properties for the expanded vertices if necessary, instead of fetch them lazily
        Set<GraphNameOrId> vertexColumns =
                Utils.extractColumnsFromRelDataType(originalGetV.getRowType(), isColumnId);
        if (isPartitioned
                && !vertexColumns.isEmpty()
                && pxd.getResultOpt() != GraphOpt.PathExpandResult.END_V) {
            // 1. build an auxilia to cache necessary properties for the start vertex of the path
            GraphAlgebraPhysical.GetV.Builder startAuxiliaBuilder =
                    GraphAlgebraPhysical.GetV.newBuilder();
            startAuxiliaBuilder.setOpt(Utils.protoGetVOpt(PhysicalGetVOpt.ITSELF));
            GraphAlgebra.QueryParams.Builder startParamsBuilder = defaultQueryParams();
            addQueryColumns(startParamsBuilder, vertexColumns);
            startAuxiliaBuilder.setParams(startParamsBuilder);
            if (pxd.getStartAlias().getAliasId() != AliasInference.DEFAULT_ID) {
                startAuxiliaBuilder.setTag(Utils.asAliasId(pxd.getStartAlias().getAliasId()));
                startAuxiliaBuilder.setAlias(Utils.asAliasId(pxd.getStartAlias().getAliasId()));
            }
            GraphAlgebraPhysical.PhysicalOpr.Builder startAuxiliaOprBuilder =
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder();
            startAuxiliaOprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                            .setVertex(startAuxiliaBuilder));
            startAuxiliaOprBuilder.addAllMetaData(
                    Utils.physicalProtoRowType(originalGetV.getRowType(), isColumnId));
            addRepartitionToAnother(pxd.getStartAlias().getAliasId());
            physicalBuilder.addPlan(startAuxiliaOprBuilder.build());
            // 2. build an auxilia to cache necessary properties for the expanded vertices during
            // the path, in expand base
            if (getVBuilder == null) {
                // getVBuilder can be null if the expand base is ExpandV
                // then create a new GetV to cache properties
                getVBuilder = buildVertex(originalGetV, PhysicalGetVOpt.ITSELF);
            }
            GraphAlgebra.QueryParams.Builder paramsBuilder = getVBuilder.getParamsBuilder();
            addQueryColumns(paramsBuilder, vertexColumns);
            getVBuilder.setParams(paramsBuilder);
            expandBaseBuilder.setGetV(getVBuilder);
        } else {
            if (getVBuilder != null) {
                expandBaseBuilder.setGetV(getVBuilder);
            }
        }
        pathExpandBuilder.setBase(expandBaseBuilder);
        pathExpandBuilder.setPathOpt(Utils.protoPathOpt(pxd.getPathOpt()));
        pathExpandBuilder.setResultOpt(Utils.protoPathResultOpt(pxd.getResultOpt()));
        GraphAlgebra.Range range = buildRange(pxd.getOffset(), pxd.getFetch());
        pathExpandBuilder.setHopRange(range);
        if (pxd.getUntilCondition() != null) {
            OuterExpression.Expression untilCondition =
                    pxd.getUntilCondition()
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
            pathExpandBuilder.setCondition(untilCondition);
        }
        if (pxd.getAliasId() != AliasInference.DEFAULT_ID) {
            pathExpandBuilder.setAlias(Utils.asAliasId(pxd.getAliasId()));
        }
        if (pxd.getStartAlias().getAliasId() != AliasInference.DEFAULT_ID) {
            pathExpandBuilder.setStartTag(Utils.asAliasId(pxd.getStartAlias().getAliasId()));
        }
        pathExpandBuilder.setIsOptional(pxd.isOptional());
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setPath(pathExpandBuilder));
        oprBuilder.addAllMetaData(Utils.physicalProtoRowType(rowType, isColumnId));
        if (isPartitioned) {
            addRepartitionToAnother(pxd.getStartAlias().getAliasId());
        }
        physicalBuilder.addPlan(oprBuilder.build());
        return pxd;
    }

    @Override
    public RelNode visit(GraphPhysicalExpand physicalExpand) {
        visitChildren(physicalExpand);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.EdgeExpand.Builder edgeExpand = buildEdgeExpand(physicalExpand);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setEdge(edgeExpand));
        // Currently we use the row type of ExpandE as the output row type of the fused
        // ExpandV, as desired by the engine implementation.
        oprBuilder.addAllMetaData(
                Utils.physicalProtoRowType(
                        physicalExpand.getFusedExpand().getRowType(), isColumnId));
        if (isPartitioned) {
            addRepartitionToAnother(physicalExpand.getStartAlias().getAliasId());
        }
        physicalBuilder.addPlan(oprBuilder.build());
        return physicalExpand;
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
        if (isPartitioned) {
            addRepartitionToAnother(physicalGetV.getStartAlias().getAliasId());
        }
        physicalBuilder.addPlan(oprBuilder.build());
        return physicalGetV;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        visitChildren(filter);
        RexNode condition = filter.getCondition();
        GraphAlgebraPhysical.PhysicalOpr.Operator.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder();
        if (isSubQueryOfExistsKind(condition)) {
            oprBuilder.setApply(
                    buildApply(
                            (RexSubQuery) condition,
                            GraphAlgebraPhysical.Join.JoinKind.SEMI,
                            AliasInference.DEFAULT_ID));
        } else if (isSubQueryOfNotExistsKind(condition)) {
            oprBuilder.setApply(
                    buildApply(
                            (RexSubQuery) ((RexCall) condition).getOperands().get(0),
                            GraphAlgebraPhysical.Join.JoinKind.ANTI,
                            AliasInference.DEFAULT_ID));
        } else {
            OuterExpression.Expression exprProto =
                    condition.accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
            oprBuilder.setSelect(GraphAlgebra.Select.newBuilder().setPredicate(exprProto));
            if (isPartitioned) {
                Map<Integer, Set<GraphNameOrId>> tagColumns =
                        Utils.extractTagColumnsFromRexNodes(List.of(filter.getCondition()));
                if (preCacheEdgeProps) {
                    // Currently, we've already precache edge properties and path properties, so we
                    // need to remove them. So as the follows.
                    Utils.removeEdgeProperties(
                            com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(
                                    filter.getInput()),
                            tagColumns);
                }
                lazyPropertyFetching(tagColumns, true);
            }
        }
        physicalBuilder.addPlan(
                GraphAlgebraPhysical.PhysicalOpr.newBuilder()
                        .setOpr(oprBuilder)
                        .addAllMetaData(
                                Utils.physicalProtoRowType(filter.getRowType(), isColumnId)));
        return filter;
    }

    private boolean isSubQueryOfExistsKind(RexNode condition) {
        return (condition instanceof RexSubQuery) && condition.getKind() == SqlKind.EXISTS;
    }

    private boolean isSubQueryOfNotExistsKind(RexNode condition) {
        return condition.getKind() == SqlKind.NOT
                && isSubQueryOfExistsKind(((RexCall) condition).getOperands().get(0));
    }

    @Override
    public RelNode visit(GraphLogicalProject project) {
        visitChildren(project);
        List<RelDataTypeField> fields = project.getRowType().getFieldList();
        List<Integer> applyColumns = Lists.newArrayList();
        List<Integer> exprColumns = Lists.newArrayList();
        for (int i = 0; i < project.getProjects().size(); ++i) {
            RexNode expr = project.getProjects().get(i);
            if (expr instanceof RexSubQuery) {
                applyColumns.add(i);
            } else {
                exprColumns.add(i);
            }
        }
        if (!applyColumns.isEmpty()) {
            for (Integer column : applyColumns) {
                GraphAlgebraPhysical.Apply.Builder applyBuilder =
                        buildApply(
                                (RexSubQuery) project.getProjects().get(column),
                                GraphAlgebraPhysical.Join.JoinKind.INNER,
                                fields.get(column).getIndex());
                physicalBuilder.addPlan(
                        GraphAlgebraPhysical.PhysicalOpr.newBuilder()
                                .setOpr(
                                        GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                                                .setApply(applyBuilder))
                                .build());
            }
        }
        if (!exprColumns.isEmpty()) {
            GraphAlgebraPhysical.Project.Builder projectBuilder =
                    GraphAlgebraPhysical.Project.newBuilder().setIsAppend(project.isAppend());
            List<RexNode> newExprs = Lists.newArrayList();
            List<RelDataTypeField> newFields = Lists.newArrayList();
            for (Integer column : exprColumns) {
                int aliasId = fields.get(column).getIndex();
                OuterExpression.Expression expression =
                        project.getProjects()
                                .get(column)
                                .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
                GraphAlgebraPhysical.Project.ExprAlias.Builder projectExprAliasBuilder =
                        GraphAlgebraPhysical.Project.ExprAlias.newBuilder();
                projectExprAliasBuilder.setExpr(expression);
                if (aliasId != AliasInference.DEFAULT_ID) {
                    projectExprAliasBuilder.setAlias(Utils.asAliasId(aliasId));
                }
                projectBuilder.addMappings(projectExprAliasBuilder);
                newExprs.add(project.getProjects().get(column));
                newFields.add(fields.get(column));
            }
            if (isPartitioned) {
                Map<Integer, Set<GraphNameOrId>> tagColumns =
                        Utils.extractTagColumnsFromRexNodes(newExprs);
                if (preCacheEdgeProps) {
                    Utils.removeEdgeProperties(
                            com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(
                                    project.getInput()),
                            tagColumns);
                }
                lazyPropertyFetching(tagColumns, true);
            }
            RelDataType newType = new RelRecordType(StructKind.FULLY_QUALIFIED, newFields);
            physicalBuilder.addPlan(
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder()
                            .setOpr(
                                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                                            .setProject(projectBuilder)
                                            .build())
                            .addAllMetaData(Utils.physicalProtoRowType(newType, isColumnId))
                            .build());
        }
        return project;
    }

    private GraphAlgebraPhysical.Apply.Builder buildApply(
            RexSubQuery query, GraphAlgebraPhysical.Join.JoinKind joinKind, int aliasId) {
        GraphAlgebraPhysical.PhysicalPlan.Builder applyPlanBuilder =
                GraphAlgebraPhysical.PhysicalPlan.newBuilder();
        query.rel.accept(
                new GraphRelToProtoConverter(
                        isColumnId,
                        graphConfig,
                        applyPlanBuilder,
                        this.relToCommons,
                        this.extraParams,
                        depth + 1));
        GraphAlgebraPhysical.Apply.Builder applyBuilder =
                GraphAlgebraPhysical.Apply.newBuilder()
                        .setSubPlan(applyPlanBuilder)
                        .setJoinKind(joinKind);
        if (aliasId != AliasInference.DEFAULT_ID) {
            applyBuilder.setAlias(Utils.asAliasId(aliasId));
        }
        return applyBuilder;
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
                if (aliasId != AliasInference.DEFAULT_ID) {
                    projectExprAliasBuilder.setAlias(Utils.asAliasId(aliasId));
                }
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
            if (isPartitioned) {
                Map<Integer, Set<GraphNameOrId>> tagColumns =
                        Utils.extractTagColumnsFromRexNodes(keys.getVariables());
                if (preCacheEdgeProps) {
                    Utils.removeEdgeProperties(
                            com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(
                                    aggregate.getInput()),
                            tagColumns);
                }
                lazyPropertyFetching(tagColumns);
            }
            physicalBuilder.addPlan(projectOprBuilder.build());
            physicalBuilder.addPlan(dedupOprBuilder.build());
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
                if (aliasId != AliasInference.DEFAULT_ID) {
                    keyAliasBuilder.setAlias(Utils.asAliasId(aliasId));
                }
                groupByBuilder.addMappings(keyAliasBuilder);
            }
            for (int i = 0; i < groupCalls.size(); ++i) {
                List<RexNode> operands = groupCalls.get(i).getOperands();
                if (operands.isEmpty()) {
                    throw new IllegalArgumentException(
                            "operands in aggregate call should not be empty");
                }

                GraphAlgebraPhysical.GroupBy.AggFunc.Builder aggFnAliasBuilder =
                        GraphAlgebraPhysical.GroupBy.AggFunc.newBuilder();
                for (RexNode operand : operands) {
                    Preconditions.checkArgument(
                            operand instanceof RexGraphVariable,
                            "each expression in aggregate call should be type %s, but is %s",
                            RexGraphVariable.class,
                            operand.getClass());
                    OuterExpression.Variable var =
                            operand.accept(
                                            new RexToProtoConverter(
                                                    true, isColumnId, this.rexBuilder))
                                    .getOperators(0)
                                    .getVar();
                    aggFnAliasBuilder.addVars(var);
                }
                GraphAlgebraPhysical.GroupBy.AggFunc.Aggregate aggOpt =
                        Utils.protoAggOpt(groupCalls.get(i));
                aggFnAliasBuilder.setAggregate(aggOpt);
                int aliasId = fields.get(i + keys.groupKeyCount()).getIndex();
                if (aliasId != AliasInference.DEFAULT_ID) {
                    aggFnAliasBuilder.setAlias(Utils.asAliasId(aliasId));
                }
                groupByBuilder.addFunctions(aggFnAliasBuilder);
            }
            oprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                            .setGroupBy(groupByBuilder));
            oprBuilder.addAllMetaData(
                    Utils.physicalProtoRowType(aggregate.getRowType(), isColumnId));
            if (isPartitioned) {
                List<RexNode> keysAndAggs = Lists.newArrayList();
                keysAndAggs.addAll(keys.getVariables());
                keysAndAggs.addAll(
                        groupCalls.stream()
                                .flatMap(k -> k.getOperands().stream())
                                .collect(Collectors.toList()));
                Map<Integer, Set<GraphNameOrId>> tagColumns =
                        Utils.extractTagColumnsFromRexNodes(keysAndAggs);
                if (preCacheEdgeProps) {
                    Utils.removeEdgeProperties(
                            com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(
                                    aggregate.getInput()),
                            tagColumns);
                }
                lazyPropertyFetching(tagColumns);
            }
            physicalBuilder.addPlan(oprBuilder.build());
        }
        return aggregate;
    }

    @Override
    public RelNode visit(GraphLogicalDedupBy dedupBy) {
        visitChildren(dedupBy);
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebra.Dedup.Builder dedupBuilder = GraphAlgebra.Dedup.newBuilder();
        for (RexNode expr : dedupBy.getDedupByKeys()) {
            Preconditions.checkArgument(
                    expr instanceof RexGraphVariable,
                    "each expression in dedup by should be type %s, but is %s",
                    RexGraphVariable.class,
                    expr.getClass());
            OuterExpression.Variable var =
                    expr.accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder))
                            .getOperators(0)
                            .getVar();
            dedupBuilder.addKeys(var);
        }
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setDedup(dedupBuilder));
        oprBuilder.addAllMetaData(Utils.physicalProtoRowType(dedupBy.getRowType(), isColumnId));
        if (isPartitioned) {
            Map<Integer, Set<GraphNameOrId>> tagColumns =
                    Utils.extractTagColumnsFromRexNodes(dedupBy.getDedupByKeys());
            if (preCacheEdgeProps) {
                Utils.removeEdgeProperties(
                        com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(
                                dedupBy.getInput()),
                        tagColumns);
            }
            lazyPropertyFetching(tagColumns);
        }
        physicalBuilder.addPlan(oprBuilder.build());
        return dedupBy;
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
            if (isPartitioned) {
                Map<Integer, Set<GraphNameOrId>> tagColumns =
                        Utils.extractTagColumnsFromRexNodes(
                                collations.stream()
                                        .map(k -> ((GraphFieldCollation) k).getVariable())
                                        .collect(Collectors.toList()));
                if (preCacheEdgeProps) {
                    Utils.removeEdgeProperties(
                            com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(
                                    sort.getInput()),
                            tagColumns);
                }
                lazyPropertyFetching(tagColumns);
            }
        } else {
            GraphAlgebra.Limit.Builder limitBuilder = GraphAlgebra.Limit.newBuilder();
            limitBuilder.setRange(buildRange(sort.offset, sort.fetch));
            oprBuilder.setOpr(
                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setLimit(limitBuilder));
        }
        physicalBuilder.addPlan(oprBuilder.build());
        return sort;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Join.Builder joinBuilder = GraphAlgebraPhysical.Join.newBuilder();
        joinBuilder.setJoinKind(Utils.protoJoinKind(join.getJoinType()));
        List<RexNode> conditions = RelOptUtil.conjunctions(join.getCondition());
        Preconditions.checkArgument(
                !conditions.isEmpty(), "join condition in physical should not be empty");
        List<RexNode> leftKeys = Lists.newArrayList();
        List<RexNode> rightKeys = Lists.newArrayList();
        for (RexNode condition : conditions) {
            List<RexGraphVariable> leftRightVars = getLeftRightVariables(condition);
            Preconditions.checkArgument(
                    leftRightVars.size() == 2,
                    "join condition in physical should have two operands, while it is %s",
                    leftRightVars.size());
            leftKeys.add(leftRightVars.get(0));
            rightKeys.add(leftRightVars.get(1));
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
        left.accept(
                new GraphRelToProtoConverter(
                        isColumnId,
                        graphConfig,
                        leftPlanBuilder,
                        this.relToCommons,
                        this.extraParams,
                        depth + 1));
        RelNode right = join.getRight();
        right.accept(
                new GraphRelToProtoConverter(
                        isColumnId,
                        graphConfig,
                        rightPlanBuilder,
                        this.relToCommons,
                        this.extraParams,
                        depth + 1));
        if (isPartitioned) {

            Map<Integer, Set<GraphNameOrId>> leftTagColumns =
                    Utils.extractTagColumnsFromRexNodes(leftKeys);
            Map<Integer, Set<GraphNameOrId>> rightTagColumns =
                    Utils.extractTagColumnsFromRexNodes(rightKeys);
            if (preCacheEdgeProps) {
                Utils.removeEdgeProperties(
                        com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(join.getLeft()),
                        leftTagColumns);
                Utils.removeEdgeProperties(
                        com.alibaba.graphscope.common.ir.tools.Utils.getOutputType(join.getRight()),
                        rightTagColumns);
            }
            lazyPropertyFetching(leftPlanBuilder, leftTagColumns, false);
            lazyPropertyFetching(rightPlanBuilder, rightTagColumns, false);
        }
        joinBuilder.setLeftPlan(leftPlanBuilder);
        joinBuilder.setRightPlan(rightPlanBuilder);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setJoin(joinBuilder));
        physicalBuilder.addPlan(oprBuilder.build());
        return join;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        List<CommonTableScan> commons = relToCommons.get(union);
        if (ObjectUtils.isNotEmpty(commons)) {
            // add commons before union
            GraphAlgebraPhysical.PhysicalPlan.Builder commonPlanBuilder =
                    GraphAlgebraPhysical.PhysicalPlan.newBuilder();
            for (int i = commons.size() - 1; i >= 0; --i) {
                RelNode commonRel = ((CommonOptTable) commons.get(i).getTable()).getCommon();
                commonRel.accept(
                        new GraphRelToProtoConverter(
                                isColumnId,
                                graphConfig,
                                commonPlanBuilder,
                                this.relToCommons,
                                this.extraParams,
                                depth + 1));
            }
            physicalBuilder.addAllPlan(commonPlanBuilder.getPlanList());
        } else if (this.depth == 0) {
            // add a dummy root if the root union does not have any common sub-plans
            physicalBuilder.addPlan(
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder()
                            .setOpr(
                                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                                            .setRoot(GraphAlgebraPhysical.Root.newBuilder())));
        }
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Union.Builder unionBuilder = GraphAlgebraPhysical.Union.newBuilder();
        for (RelNode input : union.getInputs()) {
            GraphAlgebraPhysical.PhysicalPlan.Builder inputPlanBuilder =
                    GraphAlgebraPhysical.PhysicalPlan.newBuilder();
            input.accept(
                    new GraphRelToProtoConverter(
                            isColumnId,
                            graphConfig,
                            inputPlanBuilder,
                            this.relToCommons,
                            this.extraParams,
                            depth + 1));
            unionBuilder.addSubPlans(inputPlanBuilder);
        }
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setUnion(unionBuilder));
        physicalBuilder.addPlan(oprBuilder.build());
        return union;
    }

    @Override
    public RelNode visit(MultiJoin multiJoin) {
        // we convert multi-join to intersect + unfold
        List<CommonTableScan> commons = relToCommons.get(multiJoin);
        if (ObjectUtils.isNotEmpty(commons)) {
            // add commons before intersect
            GraphAlgebraPhysical.PhysicalPlan.Builder commonPlanBuilder =
                    GraphAlgebraPhysical.PhysicalPlan.newBuilder();
            for (int i = commons.size() - 1; i >= 0; --i) {
                RelNode commonRel = ((CommonOptTable) commons.get(i).getTable()).getCommon();
                commonRel.accept(
                        new GraphRelToProtoConverter(
                                isColumnId,
                                graphConfig,
                                commonPlanBuilder,
                                this.relToCommons,
                                this.extraParams,
                                depth + 1));
            }
            physicalBuilder.addAllPlan(commonPlanBuilder.getPlanList());
        }
        GraphAlgebraPhysical.PhysicalOpr.Builder intersectOprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Intersect.Builder intersectBuilder =
                GraphAlgebraPhysical.Intersect.newBuilder();

        List<RexNode> conditions = RelOptUtil.conjunctions(multiJoin.getJoinFilter());
        int intersectKey = -1;
        for (RexNode condition : conditions) {
            List<RexGraphVariable> leftRightVars = getLeftRightVariables(condition);
            Preconditions.checkArgument(
                    leftRightVars.size() == 2,
                    "the condition of multi-join" + " should be equal condition");
            if (intersectKey == -1) {
                intersectKey = leftRightVars.get(0).getAliasId();
            } else {
                Preconditions.checkArgument(
                        intersectKey == leftRightVars.get(0).getAliasId(),
                        "the intersect key should be the same in multi-join: "
                                + intersectKey
                                + " "
                                + leftRightVars.get(0).getAliasId());
            }
        }
        Preconditions.checkArgument(intersectKey != -1, "intersect key should be set");
        intersectBuilder.setKey(intersectKey);

        // then, build subplans for intersect
        for (RelNode input : multiJoin.getInputs()) {
            GraphAlgebraPhysical.PhysicalPlan.Builder subPlanBuilder =
                    GraphAlgebraPhysical.PhysicalPlan.newBuilder();
            input.accept(
                    new GraphRelToProtoConverter(
                            isColumnId,
                            graphConfig,
                            subPlanBuilder,
                            this.relToCommons,
                            this.extraParams,
                            depth + 1));
            intersectBuilder.addSubPlans(subPlanBuilder);
        }
        intersectOprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                        .setIntersect(intersectBuilder));
        physicalBuilder.addPlan(intersectOprBuilder.build());
        return multiJoin;
    }

    @Override
    public RelNode visit(GraphLogicalUnfold unfold) {
        visitChildren(unfold);
        RexNode unfoldKey = unfold.getUnfoldKey();
        Preconditions.checkArgument(
                unfoldKey instanceof RexGraphVariable,
                "unfold key should be a variable, but is [%s]",
                unfoldKey);
        int keyAliasId = ((RexGraphVariable) unfoldKey).getAliasId();
        GraphAlgebraPhysical.Unfold.Builder unfoldBuilder =
                GraphAlgebraPhysical.Unfold.newBuilder().setTag(Utils.asAliasId(keyAliasId));
        if (unfold.getAliasId() != AliasInference.DEFAULT_ID) {
            unfoldBuilder.setAlias(Utils.asAliasId(unfold.getAliasId()));
        }
        List<RelDataTypeField> fullFields = unfold.getRowType().getFieldList();
        Preconditions.checkArgument(!fullFields.isEmpty(), "there is no fields in unfold row type");
        RelDataType curRowType =
                new RelRecordType(
                        StructKind.FULLY_QUALIFIED,
                        fullFields.subList(fullFields.size() - 1, fullFields.size()));
        physicalBuilder.addPlan(
                GraphAlgebraPhysical.PhysicalOpr.newBuilder()
                        .setOpr(
                                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                                        .setUnfold(unfoldBuilder)
                                        .build())
                        .addAllMetaData(Utils.physicalProtoRowType(curRowType, isColumnId))
                        .build());
        return unfold;
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
            GraphLogicalExpand expand, GraphOpt.PhysicalExpandOpt opt, int aliasId) {
        GraphAlgebraPhysical.EdgeExpand.Builder expandBuilder =
                GraphAlgebraPhysical.EdgeExpand.newBuilder();
        expandBuilder.setDirection(Utils.protoExpandDirOpt(expand.getOpt()));
        GraphAlgebra.QueryParams.Builder queryParamsBuilder = buildQueryParams(expand);
        if (preCacheEdgeProps && GraphOpt.PhysicalExpandOpt.EDGE.equals(opt)) {
            addQueryColumns(
                    queryParamsBuilder,
                    Utils.extractColumnsFromRelDataType(expand.getRowType(), isColumnId));
        }
        expandBuilder.setParams(queryParamsBuilder);
        if (aliasId != AliasInference.DEFAULT_ID) {
            expandBuilder.setAlias(Utils.asAliasId(aliasId));
        }
        if (expand.getStartAlias().getAliasId() != AliasInference.DEFAULT_ID) {
            expandBuilder.setVTag(Utils.asAliasId(expand.getStartAlias().getAliasId()));
        }
        expandBuilder.setExpandOpt(Utils.protoExpandOpt(opt));
        expandBuilder.setIsOptional(expand.isOptional());
        return expandBuilder;
    }

    private GraphAlgebraPhysical.EdgeExpand.Builder buildEdgeExpand(GraphLogicalExpand expand) {
        return buildEdgeExpand(expand, GraphOpt.PhysicalExpandOpt.EDGE, expand.getAliasId());
    }

    private GraphAlgebraPhysical.EdgeExpand.Builder buildEdgeExpand(
            GraphPhysicalExpand physicalExpand) {
        return buildEdgeExpand(
                physicalExpand.getFusedExpand(),
                physicalExpand.getPhysicalOpt(),
                physicalExpand.getAliasId());
    }

    private GraphAlgebraPhysical.GetV.Builder buildVertex(
            GraphLogicalGetV getV, GraphOpt.PhysicalGetVOpt opt) {
        GraphAlgebraPhysical.GetV.Builder vertexBuilder = GraphAlgebraPhysical.GetV.newBuilder();
        vertexBuilder.setOpt(Utils.protoGetVOpt(opt));
        vertexBuilder.setParams(buildQueryParams(getV));
        if (getV.getAliasId() != AliasInference.DEFAULT_ID) {
            vertexBuilder.setAlias(Utils.asAliasId(getV.getAliasId()));
        }
        if (getV.getStartAlias().getAliasId() != AliasInference.DEFAULT_ID) {
            vertexBuilder.setTag(Utils.asAliasId(getV.getStartAlias().getAliasId()));
        }
        return vertexBuilder;
    }

    private GraphAlgebraPhysical.GetV.Builder buildGetV(GraphLogicalGetV getV) {
        return buildVertex(getV, PhysicalGetVOpt.valueOf(getV.getOpt().name()));
    }

    private GraphAlgebraPhysical.GetV.Builder buildAuxilia(GraphPhysicalGetV getV) {
        return buildVertex(getV.getFusedGetV(), PhysicalGetVOpt.ITSELF);
    }

    private GraphAlgebra.Range buildRange(RexNode offset, RexNode fetch) {
        if (offset != null && !(offset instanceof RexLiteral)
                || fetch != null && !(fetch instanceof RexLiteral)) {
            throw new IllegalArgumentException(
                    "can not get INTEGER hops from types instead of RexLiteral");
        }
        GraphAlgebra.Range.Builder rangeBuilder = GraphAlgebra.Range.newBuilder();
        int lower = (offset == null) ? 0 : ((Number) ((RexLiteral) offset).getValue()).intValue();
        rangeBuilder.setLower(lower);
        rangeBuilder.setUpper(
                fetch == null
                        ? Integer.MAX_VALUE
                        : lower + ((Number) ((RexLiteral) fetch).getValue()).intValue());
        return rangeBuilder.build();
    }

    private GraphAlgebra.IndexPredicate buildIndexPredicates(RexNode uniqueKeyFilters) {
        GraphAlgebra.IndexPredicate indexPredicate =
                uniqueKeyFilters.accept(
                        new RexToIndexPbConverter(true, this.isColumnId, this.rexBuilder));
        return indexPredicate;
    }

    private GraphAlgebra.QueryParams.Builder defaultQueryParams() {
        GraphAlgebra.QueryParams.Builder paramsBuilder = GraphAlgebra.QueryParams.newBuilder();
        // TODO: currently no sample rate fused into tableScan, so directly set 1.0 as default.
        paramsBuilder.setSampleRatio(1.0);
        if (!this.extraParams.isEmpty()) {
            paramsBuilder.putAllExtra(extraParams);
        }
        return paramsBuilder;
    }

    private void addQueryTables(
            GraphAlgebra.QueryParams.Builder paramsBuilder, List<GraphLabelType.Entry> labels) {
        Set<Integer> uniqueLabelIds =
                labels.stream().map(k -> k.getLabelId()).collect(Collectors.toSet());
        uniqueLabelIds.forEach(
                k -> {
                    paramsBuilder.addTables(Utils.asNameOrId(k));
                });
    }

    private void addQueryFilters(
            GraphAlgebra.QueryParams.Builder paramsBuilder,
            @Nullable ImmutableList<RexNode> filters) {
        if (ObjectUtils.isNotEmpty(filters)) {
            OuterExpression.Expression expression =
                    filters.get(0)
                            .accept(new RexToProtoConverter(true, isColumnId, this.rexBuilder));
            paramsBuilder.setPredicate(expression);
        }
    }

    private void addQueryColumns(
            GraphAlgebra.QueryParams.Builder paramsBuilder, Set<GraphNameOrId> columns) {
        for (GraphNameOrId column : columns) {
            paramsBuilder.addColumns(Utils.protoNameOrId(column));
        }
    }

    private GraphAlgebra.QueryParams.Builder buildQueryParams(AbstractBindableTableScan tableScan) {
        GraphAlgebra.QueryParams.Builder paramsBuilder = defaultQueryParams();
        addQueryTables(
                paramsBuilder,
                com.alibaba.graphscope.common.ir.tools.Utils.getGraphLabels(tableScan.getRowType())
                        .getLabelsEntry());
        addQueryFilters(paramsBuilder, tableScan.getFilters());
        return paramsBuilder;
    }

    private void addRepartitionToAnother(int repartitionKey) {
        addRepartitionToAnother(physicalBuilder, repartitionKey);
    }

    private void addRepartitionToAnother(
            GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder, int repartitionKey) {
        GraphAlgebraPhysical.PhysicalOpr.Builder repartitionOprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Repartition repartition =
                Utils.protoShuffleRepartition(repartitionKey);
        repartitionOprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setRepartition(repartition));
        physicalBuilder.addPlan(repartitionOprBuilder.build());
    }

    private void addAuxilia(
            GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder,
            Integer tag,
            Set<GraphNameOrId> columns) {
        GraphAlgebraPhysical.PhysicalOpr.Builder auxiliaOprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.GetV.Builder vertexBuilder = GraphAlgebraPhysical.GetV.newBuilder();
        vertexBuilder.setOpt(Utils.protoGetVOpt(PhysicalGetVOpt.ITSELF));
        GraphAlgebra.QueryParams.Builder paramsBuilder = defaultQueryParams();
        addQueryColumns(paramsBuilder, columns);
        vertexBuilder.setParams(paramsBuilder);
        if (tag != AliasInference.DEFAULT_ID) {
            vertexBuilder.setTag(Utils.asAliasId(tag));
            vertexBuilder.setAlias(Utils.asAliasId(tag));
        }
        auxiliaOprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setVertex(vertexBuilder));
        physicalBuilder.addPlan(auxiliaOprBuilder.build());
    }

    private void lazyPropertyFetching(Map<Integer, Set<GraphNameOrId>> columns) {
        // by default, we cache the result of the tagColumns, i.e., the optimizedNoCaching is false
        lazyPropertyFetching(columns, false);
    }

    private void lazyPropertyFetching(
            Map<Integer, Set<GraphNameOrId>> tagColumns, boolean optimizedNoCaching) {
        lazyPropertyFetching(physicalBuilder, tagColumns, optimizedNoCaching);
    }

    private void lazyPropertyFetching(
            GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder,
            Map<Integer, Set<GraphNameOrId>> tagColumns,
            boolean optimizedNoCaching) {
        if (tagColumns.isEmpty()) {
            return;
        } else if (tagColumns.size() == 1 && optimizedNoCaching) {
            addRepartitionToAnother(physicalBuilder, tagColumns.keySet().iterator().next());
        } else {
            for (Map.Entry<Integer, Set<GraphNameOrId>> tagColumn : tagColumns.entrySet()) {
                addRepartitionToAnother(physicalBuilder, tagColumn.getKey());
                addAuxilia(physicalBuilder, tagColumn.getKey(), tagColumn.getValue());
            }
        }
    }

    @Override
    public RelNode visit(GraphLogicalSingleMatch match) {
        throw new UnsupportedOperationException(
                "converting logical match to physical plan is unsupported yet");
    }

    @Override
    public RelNode visit(GraphLogicalMultiMatch match) {
        throw new UnsupportedOperationException(
                "converting logical match to physical plan is unsupported yet");
    }
}
