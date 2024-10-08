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

import com.alibaba.graphscope.common.ir.meta.glogue.DetailedExpandCost;
import com.alibaba.graphscope.common.ir.rel.GraphShuttle;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphSchemaType;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class GraphLogicalExpand extends AbstractBindableTableScan {
    private final GraphOpt.Expand opt;
    private final boolean optional;

    protected GraphLogicalExpand(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphOpt.Expand opt,
            TableConfig tableConfig,
            @Nullable String alias,
            AliasNameWithId startAlias,
            boolean optional) {
        super(cluster, hints, input, tableConfig, alias, startAlias);
        this.opt = opt;
        this.optional = optional;
    }

    public static GraphLogicalExpand create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphOpt.Expand opt,
            TableConfig tableConfig,
            @Nullable String alias,
            AliasNameWithId startAlias) {
        return create(cluster, hints, input, opt, tableConfig, alias, startAlias, false);
    }

    public static GraphLogicalExpand create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphOpt.Expand opt,
            TableConfig tableConfig,
            @Nullable String alias,
            AliasNameWithId startAlias,
            boolean optional) {
        return new GraphLogicalExpand(
                cluster, hints, input, opt, tableConfig, alias, startAlias, optional);
    }

    public GraphOpt.Expand getOpt() {
        return this.opt;
    }

    public boolean isOptional() {
        return this.optional;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("opt", getOpt()).itemIf("optional", optional, optional);
    }

    @Override
    public GraphLogicalExpand copy(RelTraitSet traitSet, List<RelNode> inputs) {
        GraphLogicalExpand copy =
                new GraphLogicalExpand(
                        (GraphOptCluster) getCluster(),
                        getHints(),
                        inputs.get(0),
                        this.getOpt(),
                        this.tableConfig,
                        this.getAliasName(),
                        this.getStartAlias(),
                        this.isOptional());
        if (ObjectUtils.isNotEmpty(this.getFilters())) {
            copy.setFilters(this.getFilters());
        }
        copy.setSchemaType((GraphSchemaType) this.getRowType().getFieldList().get(0).getType());
        copy.setCachedCost(this.cachedCost);
        return copy;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return cachedCost instanceof DetailedExpandCost
                ? ((DetailedExpandCost) cachedCost).getExpandFilteringRows()
                : super.estimateRowCount(mq);
    }
}
