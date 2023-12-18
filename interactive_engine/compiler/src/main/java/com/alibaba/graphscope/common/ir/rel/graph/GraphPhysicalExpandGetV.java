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
import java.util.Objects;

// fuse expand + getv after applying 'ExpandGetVFusionRule'
public class GraphPhysicalExpandGetV extends SingleRel {
    private final GraphLogicalExpand fusedExpand;
    private final GraphLogicalGetV fusedGetV;
    private final String aliasName;
    private final int aliasId;

    protected GraphPhysicalExpandGetV(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalExpand fusedExpand,
            GraphLogicalGetV fusedGetV,
            @Nullable String aliasName) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.fusedExpand = Objects.requireNonNull(fusedExpand);
        this.fusedGetV = Objects.requireNonNull(fusedGetV);
        this.aliasName =
                AliasInference.inferDefault(
                        aliasName, AliasInference.getUniqueAliasList(input, true));
        this.aliasId = cluster.getIdGenerator().generate(this.aliasName);
    }

    public static GraphPhysicalExpandGetV create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalExpand innerExpand,
            GraphLogicalGetV innerGetV,
            @Nullable String alias) {
        return new GraphPhysicalExpandGetV(cluster, hints, input, innerExpand, innerGetV, alias);
    }

    @Override
    public RelDataType deriveRowType() {
        return fusedGetV.deriveRowType();
    }

    public GraphLogicalExpand getFusedExpand() {
        return fusedExpand;
    }

    public GraphLogicalGetV getFusedGetV() {
        return fusedGetV;
    }

    public String getAliasName() {
        return aliasName;
    }

    public int getAliasId() {
        return aliasId;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.itemIf("input", input, !Objects.isNull(input))
                .item("tableConfig", fusedExpand.tableConfig)
                .item("alias", AliasInference.SIMPLE_NAME(getAliasName()))
                .itemIf(
                    "startAlias",
                    fusedExpand.getStartAlias().getAliasName(),
                    fusedExpand.getStartAlias().getAliasName() != AliasInference.DEFAULT_NAME)
              
                .itemIf(
                        "fusedProject",
                        fusedExpand.project,
                        !ObjectUtils.isEmpty(fusedExpand.project))
                .itemIf(
                        "fusedFilter",
                        fusedExpand.filters,
                        !ObjectUtils.isEmpty(fusedExpand.filters));
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }
}
