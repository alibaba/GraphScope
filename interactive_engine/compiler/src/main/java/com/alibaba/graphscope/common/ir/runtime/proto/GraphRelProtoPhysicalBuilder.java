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
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.meta.SnapshotId;
import com.alibaba.graphscope.common.ir.meta.schema.CommonOptTable;
import com.alibaba.graphscope.common.ir.rel.CommonTableScan;
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.gaia.proto.GraphAlgebra;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.util.JsonFormat;

import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * build physical plan from logical plan of a regular query, the physical plan
 * is a protobuf message {@code GraphAlgebraPhysical.PhysicalPlan}
 */
public class GraphRelProtoPhysicalBuilder extends PhysicalBuilder {
    private static final Logger logger =
            LoggerFactory.getLogger(GraphRelProtoPhysicalBuilder.class);
    private final GraphShuttle relShuttle;
    private final GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder;
    // map each rel (union/join...) to its corresponding common sub-plans, i.e. in query
    // `g.V().out().union(out(), out())`,
    // `g.V().out()` is a common sub-plan, the pair of <union, g.V().out()> is recorded in this map
    private final IdentityHashMap<RelNode, List<CommonTableScan>> relToCommons;
    private final boolean skipSinkColumns;

    public GraphRelProtoPhysicalBuilder(
            Configs graphConfig, IrMeta irMeta, LogicalPlan logicalPlan) {
        this(graphConfig, irMeta, logicalPlan, false);
    }

    @VisibleForTesting
    public GraphRelProtoPhysicalBuilder(
            Configs graphConfig, IrMeta irMeta, LogicalPlan logicalPlan, boolean skipSinkColumns) {
        super(logicalPlan);
        this.physicalBuilder = GraphAlgebraPhysical.PhysicalPlan.newBuilder();
        this.relToCommons = createRelToCommons(logicalPlan);
        this.relShuttle =
                new GraphRelToProtoConverter(
                        irMeta.getSchema().isColumnId(),
                        graphConfig,
                        this.physicalBuilder,
                        this.relToCommons,
                        createExtraParams(irMeta));
        this.skipSinkColumns = skipSinkColumns;
    }

    @Override
    public PhysicalPlan build() {
        String plan = null;
        try {
            RelNode regularQuery = this.logicalPlan.getRegularQuery();
            regularQuery.accept(this.relShuttle);
            physicalBuilder.addPlan(
                    GraphAlgebraPhysical.PhysicalOpr.newBuilder()
                            .setOpr(
                                    GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder()
                                            .setSink(getSinkByColumns(regularQuery))));
            plan = getPlanAsJson(physicalBuilder.build());
            int planId = Objects.hash(logicalPlan);
            physicalBuilder.setPlanId(planId);
            GraphAlgebraPhysical.PhysicalPlan physicalPlan = physicalBuilder.build();
            byte[] bytes = physicalPlan.toByteArray();
            return new PhysicalPlan(bytes, plan);
        } catch (Exception e) {
            logger.error("ir physical plan {}, error {}", plan, e);
            throw new RuntimeException(e);
        }
    }

    private GraphAlgebraPhysical.Sink getSinkByColumns(RelNode regularQuery) {
        GraphAlgebraPhysical.Sink.Builder sinkBuilder = GraphAlgebraPhysical.Sink.newBuilder();
        sinkBuilder.setSinkTarget(
                GraphAlgebra.Sink.SinkTarget.newBuilder()
                        .setSinkDefault(GraphAlgebra.SinkDefault.newBuilder().build()));
        regularQuery
                .getRowType()
                .getFieldList()
                .forEach(
                        k -> {
                            if (!skipSinkColumns && k.getIndex() != AliasInference.DEFAULT_ID) {
                                sinkBuilder.addTags(
                                        GraphAlgebraPhysical.Sink.OptTag.newBuilder()
                                                .setTag(Utils.asAliasId(k.getIndex())));
                            }
                        });
        return sinkBuilder.build();
    }

    private String getPlanAsJson(GraphAlgebraPhysical.PhysicalPlan physicalPlan) {
        try {
            return JsonFormat.printer().print(physicalPlan);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {}

    private IdentityHashMap<RelNode, List<CommonTableScan>> createRelToCommons(
            LogicalPlan logicalPlan) {
        IdentityHashMap<RelNode, List<CommonTableScan>> relToCommons = new IdentityHashMap<>();
        RelNode top = logicalPlan.getRegularQuery();
        if (top == null) return relToCommons;
        List<RelNode> rels = Lists.newArrayList(top);
        List<CommonTableScan> commons = Lists.newArrayList();
        while (!rels.isEmpty()) {
            List<CommonTableScan> inputCommons = getInputCommons(rels.remove(0));
            commons.addAll(inputCommons);
            inputCommons.forEach(
                    k -> {
                        rels.add(((CommonOptTable) k.getTable()).getCommon());
                    });
        }
        // use linked hash map to keep the order of commons
        Map<RelDigest, List<CommonTableScan>> digestToCommons = Maps.newLinkedHashMap();
        commons.forEach(
                k -> {
                    RelDigest digest = k.getRelDigest();
                    digestToCommons.computeIfAbsent(digest, k1 -> Lists.newArrayList()).add(k);
                });
        digestToCommons.forEach(
                (k, v) -> {
                    if (v.size() > 1) {
                        RelNode ancestor = lowestCommonAncestor(top, v, Lists.newArrayList());
                        Preconditions.checkArgument(
                                ancestor != null,
                                "lowest common ancestor of [%s] should not be null",
                                v);
                        relToCommons
                                .computeIfAbsent(ancestor, k1 -> Lists.newArrayList())
                                .add(v.get(0));
                    }
                });
        return relToCommons;
    }

    private HashMap<String, String> createExtraParams(IrMeta irMeta) {
        HashMap<String, String> extraParams = new HashMap<>();
        // prepare extra params for physical plan, e.g. snapshot id
        SnapshotId snapshotId = irMeta.getSnapshotId();
        if (snapshotId.isAcquired()) {
            extraParams.put("SID", String.valueOf(snapshotId.getId()));
        }
        return extraParams;
    }

    /**
     * find the lowest common ancestor (union/join...) of a list of common table scans
     * @param top
     * @param commons
     * @param contains
     * @return
     */
    private @Nullable RelNode lowestCommonAncestor(
            RelNode top, List<CommonTableScan> commons, List<CommonTableScan> contains) {
        List<List<CommonTableScan>> inputContains = Lists.newArrayList();
        for (RelNode input : top.getInputs()) {
            inputContains.add(Lists.newArrayList());
            RelNode inputAncestor =
                    lowestCommonAncestor(
                            input, commons, inputContains.get(inputContains.size() - 1));
            if (inputAncestor != null) {
                return inputAncestor;
            }
        }
        if (top instanceof CommonTableScan) {
            CommonTableScan common = (CommonTableScan) top;
            inputContains.add(Lists.newArrayList());
            lowestCommonAncestor(
                    ((CommonOptTable) common.getTable()).getCommon(),
                    commons,
                    inputContains.get(inputContains.size() - 1));
        }
        inputContains.forEach(k -> contains.addAll(k));
        if (top instanceof CommonTableScan && commons.contains(top)) {
            contains.add((CommonTableScan) top);
        }
        if (contains.size() >= commons.size() && contains.containsAll(commons)) {
            return top;
        }
        return null;
    }

    private List<CommonTableScan> getInputCommons(RelNode top) {
        List<CommonTableScan> inputCommons = Lists.newArrayList();
        if (top instanceof CommonTableScan) {
            inputCommons.add((CommonTableScan) top);
        } else {
            top.getInputs().forEach(k -> inputCommons.addAll(getInputCommons(k)));
        }
        return inputCommons;
    }
}
