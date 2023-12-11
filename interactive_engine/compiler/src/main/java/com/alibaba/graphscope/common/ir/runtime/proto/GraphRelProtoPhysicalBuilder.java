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
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.runtime.PhysicalBuilder;
import com.alibaba.graphscope.common.ir.runtime.PhysicalPlan;
import com.alibaba.graphscope.common.ir.runtime.type.PhysicalNode;
import com.alibaba.graphscope.common.ir.tools.LogicalPlan;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.gaia.proto.GraphAlgebra;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical.PhysicalOpr;
import com.google.protobuf.util.JsonFormat;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * build physical plan from logical plan of a regular query, the physical plan
 * is a protobuf message {@code GraphAlgebraPhysical.PhysicalPlan}
 */
public class GraphRelProtoPhysicalBuilder extends PhysicalBuilder {
    private static final Logger logger =
            LoggerFactory.getLogger(GraphRelProtoPhysicalBuilder.class);
    protected GraphShuttle relShuttle;
    GraphAlgebraPhysical.PhysicalPlan.Builder physicalBuilder;

    public GraphRelProtoPhysicalBuilder(
            Configs graphConfig, IrMeta irMeta, LogicalPlan logicalPlan) {
        super(logicalPlan);
        this.relShuttle =
                new GraphRelToProtoConverter(irMeta.getSchema().isColumnId(), graphConfig);
        this.physicalBuilder = GraphAlgebraPhysical.PhysicalPlan.newBuilder();
    }

    @Override
    public PhysicalPlan build() {
        String plan = null;
        try {
            RelNode regularQuery = this.logicalPlan.getRegularQuery();
            RelVisitor relVisitor =
                    new RelVisitor() {
                        @Override
                        public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                            if (node.getInputs().size() == 1) {
                                super.visit(node, ordinal, parent);
                            }
                            PhysicalNode<PhysicalOpr> physicalNode =
                                    (PhysicalNode<PhysicalOpr>) node.accept(relShuttle);
                            for (PhysicalOpr opr : physicalNode.getNodes()) {
                                physicalBuilder.addPlan(opr);
                            }
                        }
                    };
            relVisitor.go(regularQuery);
            appendDefaultSink();
            plan = getPlanAsJson(physicalBuilder.build());
            int planId = Objects.hash(logicalPlan);
            physicalBuilder.setPlanId(planId);
            GraphAlgebraPhysical.PhysicalPlan physicalPlan = physicalBuilder.build();
            byte[] bytes = physicalPlan.toByteArray();
            return new PhysicalPlan(bytes, plan);
        } catch (Exception e) {
            logger.error("ir physical plan {}", plan);
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
    public void close() throws Exception {}
}
