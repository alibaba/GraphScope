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
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class GraphLogicalSource extends AbstractBindableTableScan {
    private final GraphOpt.Source opt;
    private @Nullable RexNode uniqueKeyFilters;

    protected GraphLogicalSource(
            GraphOptCluster cluster,
            List<RelHint> hints,
            GraphOpt.Source opt,
            TableConfig tableConfig,
            @Nullable String alias) {
        super(cluster, hints, tableConfig, alias);
        this.opt = opt;
    }

    public static GraphLogicalSource create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            GraphOpt.Source opt,
            TableConfig tableConfig,
            @Nullable String alias) {
        return new GraphLogicalSource(cluster, hints, opt, tableConfig, alias);
    }

    public static GraphLogicalSource create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            GraphOpt.Source opt,
            TableConfig tableConfig,
            @Nullable String alias,
            RexNode uniqueKeyFilters,
            ImmutableList<RexNode> filters) {
        GraphLogicalSource source =
                GraphLogicalSource.create(cluster, hints, opt, tableConfig, alias);
        if (uniqueKeyFilters != null) {
            source.setUniqueKeyFilters(uniqueKeyFilters);
        }
        if (ObjectUtils.isNotEmpty(filters)) {
            source.setFilters(filters);
        }
        return source;
    }

    public GraphOpt.Source getOpt() {
        return this.opt;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("opt", getOpt())
                .itemIf("uniqueKeyFilters", uniqueKeyFilters, uniqueKeyFilters != null);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    public void setUniqueKeyFilters(RexNode uniqueKeyFilters) {
        this.uniqueKeyFilters = Objects.requireNonNull(uniqueKeyFilters);
    }

    public @Nullable RexNode getUniqueKeyFilters() {
        return uniqueKeyFilters;
    }
}
