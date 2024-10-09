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

package org.apache.calcite.plan;

import com.alibaba.graphscope.common.ir.tools.AliasIdGenerator;
import com.alibaba.graphscope.gremlin.Utils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Extends from {@code RelOptCluster} to carry two more arguments:
 * one to generate alias id, another to generate {@code RelNode} id
 */
public class GraphOptCluster extends RelOptCluster {
    // to generate alias id increasingly in one query
    private final AliasIdGenerator idGenerator;

    // maintain local state for some operators specifically
    private final LocalState localState;

    protected GraphOptCluster(
            RelOptPlanner planner,
            RelDataTypeFactory typeFactory,
            RexBuilder rexBuilder,
            AtomicInteger nextCorrel,
            Map<String, RelNode> mapCorrelToRel,
            AliasIdGenerator idGenerator) {
        this(
                planner,
                typeFactory,
                rexBuilder,
                nextCorrel,
                mapCorrelToRel,
                idGenerator,
                new LocalState());
    }

    protected GraphOptCluster(
            RelOptPlanner planner,
            RelDataTypeFactory typeFactory,
            RexBuilder rexBuilder,
            AtomicInteger nextCorrel,
            Map<String, RelNode> mapCorrelToRel,
            AliasIdGenerator idGenerator,
            LocalState localState) {
        super(planner, typeFactory, rexBuilder, nextCorrel, mapCorrelToRel);
        this.idGenerator = idGenerator;
        this.localState = localState;
    }

    public static GraphOptCluster create(RelOptPlanner planner, RexBuilder rexBuilder) {
        return new GraphOptCluster(
                planner,
                rexBuilder.getTypeFactory(),
                rexBuilder,
                new AtomicInteger(0),
                new HashMap<>(),
                new AliasIdGenerator());
    }

    public GraphOptCluster copy(LocalState localState) {
        GraphOptCluster copy =
                new GraphOptCluster(
                        getPlanner(),
                        getTypeFactory(),
                        getRexBuilder(),
                        Utils.getFieldValue(RelOptCluster.class, this, "nextCorrel"),
                        Utils.getFieldValue(RelOptCluster.class, this, "mapCorrelToRel"),
                        idGenerator,
                        localState);
        copy.setMetadataQuerySupplier(this.getMetadataQuerySupplier());
        return copy;
    }

    public AliasIdGenerator getIdGenerator() {
        return idGenerator;
    }

    public LocalState getLocalState() {
        return this.localState;
    }
}
