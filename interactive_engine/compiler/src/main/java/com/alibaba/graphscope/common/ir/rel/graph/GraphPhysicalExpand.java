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
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.*;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class GraphPhysicalExpand extends SingleRel {
    private final GraphOpt.PhysicalExpandOpt physicalOpt;
    private final GraphLogicalExpand fusedExpand;
    private final GraphLogicalGetV fusedGetV;

    protected GraphPhysicalExpand(
            GraphOptCluster cluster,
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
            RelNode input,
            GraphLogicalExpand innerExpand,
            GraphLogicalGetV innerGetV,
            @Nullable String aliasName,
            GraphOpt.PhysicalExpandOpt physicalOpt) {
        GraphLogicalGetV newGetV;
        // build a new getV if a new aliasName is given, to make sure the derived row type is
        // correct (which is derived by getV)
        if (innerGetV.getAliasName().equals(aliasName)) {
            newGetV = innerGetV;
        } else {
            newGetV =
                    GraphLogicalGetV.create(
                            (GraphOptCluster) innerGetV.getCluster(),
                            innerGetV.getHints(),
                            innerGetV.getInput(0),
                            innerGetV.getOpt(),
                            innerGetV.getTableConfig(),
                            aliasName,
                            innerGetV.getStartAlias());
        }
        return new GraphPhysicalExpand(
                (GraphOptCluster) innerExpand.getCluster(),
                innerExpand.getHints(),
                input,
                innerExpand,
                newGetV,
                physicalOpt);
    }

    public GraphOpt.PhysicalExpandOpt getPhysicalOpt() {
        return this.physicalOpt;
    }

    public GraphLogicalExpand getFusedExpand() {
        return fusedExpand;
    }

    public String getAliasName() {
        return fusedGetV.getAliasName();
    }

    public int getAliasId() {
        return fusedGetV.getAliasId();
    }

    @Override
    public RelDataType deriveRowType() {
        return fusedGetV.deriveRowType();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
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
