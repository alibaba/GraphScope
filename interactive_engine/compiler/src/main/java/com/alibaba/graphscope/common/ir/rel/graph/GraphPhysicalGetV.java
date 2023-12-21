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
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class GraphPhysicalGetV extends GraphLogicalGetV {
    private final GraphOpt.PhysicalGetVOpt physicalOpt;

    protected GraphPhysicalGetV(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphOpt.GetV opt,
            TableConfig tableConfig,
            @Nullable String alias,
            AliasNameWithId startAlias,
            @Nullable ImmutableList<RexNode> filters,
            GraphOpt.PhysicalGetVOpt physicalOpt) {
        super(cluster, hints, input, opt, tableConfig, alias, startAlias);
        if (filters != null) {
            setFilters(filters);
        }
        this.physicalOpt = physicalOpt;
    }

    public static GraphPhysicalGetV create(
            RelNode input, GraphLogicalGetV innerGetV, GraphOpt.PhysicalGetVOpt physicalOpt) {
        return new GraphPhysicalGetV(
                (GraphOptCluster) innerGetV.getCluster(),
                innerGetV.getHints(),
                input,
                innerGetV.getOpt(),
                innerGetV.getTableConfig(),
                innerGetV.getAliasName(),
                innerGetV.getStartAlias(),
                innerGetV.getFilters(),
                physicalOpt);
    }

    public GraphOpt.PhysicalGetVOpt getPhysicalOpt() {
        return this.physicalOpt;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("physicalOpt", getPhysicalOpt());
    }

    @Override
    public GraphPhysicalGetV copy(RelTraitSet traitSet, List<RelNode> inputs) {
        GraphPhysicalGetV copy =
                new GraphPhysicalGetV(
                        (GraphOptCluster) getCluster(),
                        getHints(),
                        inputs.get(0),
                        this.getOpt(),
                        this.getTableConfig(),
                        this.getAliasName(),
                        this.getStartAlias(),
                        getFilters(),
                        getPhysicalOpt());
        return copy;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }
}
