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

package com.alibaba.graphscope.common.ir.rel.graph;

import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class GraphPhysicalExpand extends SingleRel {
    private final GraphOpt.PhysicalExpandOpt physicalOpt;
    private final GraphLogicalExpand fusedExpand;
    private final GraphLogicalGetV fusedGetV;

    protected GraphPhysicalExpand(
            RelOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalExpand fusedExpand,
            GraphLogicalGetV fusedGetV,
            GraphOpt.PhysicalExpandOpt physicalOpt) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.physicalOpt = physicalOpt;
        this.fusedExpand = fusedExpand;
        this.fusedGetV = fusedGetV;
    }

    public static GraphPhysicalExpand create(
            RelOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalExpand fusedExpand,
            GraphLogicalGetV fusedGetV,
            GraphOpt.PhysicalExpandOpt physicalOpt) {
        return new GraphPhysicalExpand(cluster, hints, input, fusedExpand, fusedGetV, physicalOpt);
    }

    public GraphOpt.PhysicalExpandOpt getPhysicalOpt() {
        return this.physicalOpt;
    }

    public GraphLogicalExpand getFusedExpand() {
        return fusedExpand;
    }

    public AliasNameWithId getStartAlias() {
        return fusedExpand.getStartAlias();
    }

    public String getAliasName() {
        return fusedGetV.getAliasName();
    }

    public int getAliasId() {
        return fusedGetV.getAliasId();
    }

    public @Nullable ImmutableList<RexNode> getFilters() {
        return fusedExpand.getFilters();
    }

    @Override
    public RelDataType deriveRowType() {
        return fusedGetV.deriveRowType();
    }

    @Override
    public List<RelNode> getInputs() {
        return this.input == null ? ImmutableList.of() : ImmutableList.of(this.input);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.itemIf("input", input, !Objects.isNull(input))
                .item("tableConfig", fusedExpand.tableConfig)
                .item("alias", AliasInference.SIMPLE_NAME(getAliasName()))
                .itemIf(
                        "startAlias",
                        fusedExpand.getStartAlias(),
                        fusedExpand.getStartAlias().getAliasName() != AliasInference.DEFAULT_NAME)
                .itemIf(
                        "fusedFilter",
                        fusedExpand.getFilters(),
                        !ObjectUtils.isEmpty(fusedExpand.getFilters()))
                .item("opt", fusedExpand.getOpt())
                .item("physicalOpt", getPhysicalOpt());
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }
}
