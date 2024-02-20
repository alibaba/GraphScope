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

import com.alibaba.graphscope.common.ir.rel.GraphRelVisitor;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class GraphLogicalGetV extends AbstractBindableTableScan {
    private final GraphOpt.GetV opt;

    protected GraphLogicalGetV(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphOpt.GetV opt,
            TableConfig tableConfig,
            @Nullable String alias,
            AliasNameWithId startAlias) {
        super(cluster, hints, input, tableConfig, alias, startAlias);
        this.opt = opt;
    }

    public static GraphLogicalGetV create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphOpt.GetV opt,
            TableConfig tableConfig,
            @Nullable String alias,
            AliasNameWithId startAlias) {
        return new GraphLogicalGetV(cluster, hints, input, opt, tableConfig, alias, startAlias);
    }

    public GraphOpt.GetV getOpt() {
        return this.opt;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("opt", getOpt());
    }

    @Override
    public GraphLogicalGetV copy(RelTraitSet traitSet, List<RelNode> inputs) {
        GraphLogicalGetV copy =
                new GraphLogicalGetV(
                        (GraphOptCluster) getCluster(),
                        getHints(),
                        inputs.get(0),
                        this.getOpt(),
                        this.tableConfig,
                        this.getAliasName(),
                        this.getStartAlias());
        if (ObjectUtils.isNotEmpty(this.getFilters())) {
            copy.setFilters(this.getFilters());
        }
        return copy;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphRelVisitor) {
            return ((GraphRelVisitor) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }
}
