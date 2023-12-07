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
import com.alibaba.graphscope.common.ir.rel.GraphRelShuttleWrapper;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalPathExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalSource;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalMultiMatch;
import com.alibaba.graphscope.common.ir.rel.graph.match.GraphLogicalSingleMatch;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.RegularPhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.type.PhysicalNode;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.GraphAlgebra;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical.PhysicalOpr;
import com.google.protobuf.util.JsonFormat;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * build physical plan from logical plan of a regular query, the physical plan
 * is a protobuf message {@code GraphAlgebraPhysical.PhysicalPlan}
 */
public class ProtoPhysicalBuilder extends RegularPhysicalBuilder<PhysicalOpr> {
    private static final Logger logger = LoggerFactory.getLogger(ProtoPhysicalBuilder.class);
    private final GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder;

    public ProtoPhysicalBuilder(Configs graphConfig, IrMeta irMeta, LogicalPlan logicalPlan) {
        super(
                logicalPlan,
                new GraphRelShuttleWrapper(
                        new RelToProtoConverter(irMeta.getSchema().isColumnId(), graphConfig)));
        this.physicalBuilder = GraphAlgebraPhysical.PhysicalPlan.newBuilder();
        initialize();
    }

    @Override
    protected void appendNode(PhysicalNode<PhysicalOpr> node) {
        RelNode original = node.getOriginal();
        if (original instanceof GraphLogicalSource
                || original instanceof GraphLogicalExpand
                || original instanceof GraphLogicalGetV
                || original instanceof GraphLogicalPathExpand
                || original instanceof GraphLogicalProject
                || original instanceof LogicalFilter
                || original instanceof GraphLogicalSort
                || original instanceof LogicalJoin) {
            physicalBuilder.addPlan(node.getNode());
        } else if (original instanceof GraphLogicalAggregate) {
            // transform aggregate to project + dedup by key
            if (((GraphLogicalAggregate) original).getAggCalls().isEmpty()) {
                for (PhysicalOpr opr : node.getNodes()) {
                    physicalBuilder.addPlan(opr);
                }
            } else {
                physicalBuilder.addPlan(node.getNode());
            }
        } else if (original instanceof GraphLogicalSingleMatch
                || original instanceof GraphLogicalMultiMatch) {
            // TODO: will not append match.
            throw new UnsupportedOperationException(
                    "node type "
                            + original.getClass()
                            + " can not be appended to the physical plan");
        } else {
            throw new UnsupportedOperationException(
                    "node type "
                            + original.getClass()
                            + " can not be appended to the physical plan");
        }
    }

    @Override
    public PhysicalPlan build() {
        String plan = null;
        try {
            appendDefaultSink();
            plan = getPlanAsJson(physicalBuilder.build());
            int planId = Objects.hash(logicalPlan);
            physicalBuilder.setPlanId(planId);
            GraphAlgebraPhysical.PhysicalPlan physicalPlan = physicalBuilder.build();
            byte[] bytes = physicalPlan.toByteArray();
            return new PhysicalPlan(bytes, plan);
        } catch (Exception e) {
            logger.error("ir core logical plan {}", plan);
            throw new RuntimeException(e);
        }
    }

    private void appendDefaultSink() {
        GraphAlgebraPhysical.PhysicalOpr.Builder oprBuilder =
                GraphAlgebraPhysical.PhysicalOpr.newBuilder();
        GraphAlgebraPhysical.Sink.Builder sinkBuilder = GraphAlgebraPhysical.Sink.newBuilder();
        sinkBuilder.addTags(GraphAlgebraPhysical.Sink.OptTag.newBuilder().build());
        GraphAlgebra.Sink.SinkTarget.Builder sinkTargetBuilder =
                GraphAlgebra.Sink.SinkTarget.newBuilder();
        sinkTargetBuilder.setSinkDefault(GraphAlgebra.SinkDefault.newBuilder().build());
        sinkBuilder.setSinkTarget(sinkTargetBuilder);
        oprBuilder.setOpr(
                GraphAlgebraPhysical.PhysicalOpr.Operator.newBuilder().setSink(sinkBuilder));
        physicalBuilder.addPlan(oprBuilder);
    }

    private String getPlanAsJson(GraphAlgebraPhysical.PhysicalPlan physicalPlan) {
        try {
            return JsonFormat.printer().print(physicalPlan);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        // TODO Auto-generated method stub
    }
}
