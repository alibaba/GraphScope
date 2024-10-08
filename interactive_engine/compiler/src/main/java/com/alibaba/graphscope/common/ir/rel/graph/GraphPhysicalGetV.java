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

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.commons.lang3.ObjectUtils;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

public class GraphPhysicalGetV extends SingleRel {
    private final GraphOpt.PhysicalGetVOpt physicalOpt;
    private final GraphLogicalGetV fusedGetV;

    protected GraphPhysicalGetV(
            RelOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalGetV fusedGetV,
            GraphOpt.PhysicalGetVOpt physicalOpt) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.physicalOpt = physicalOpt;
        this.fusedGetV = fusedGetV;
    }

    public static GraphPhysicalGetV create(
            RelOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalGetV innerGetV,
            String aliasName,
            GraphOpt.PhysicalGetVOpt physicalOpt) {
        // build a new getV if a new aliasName is given, to make sure the derived row type is
        // correct (which is derived by getV)
        GraphLogicalGetV newGetV;
        if (innerGetV.getAliasName().equals(aliasName)) {
            newGetV = innerGetV;
        } else {
            newGetV =
                    GraphLogicalGetV.create(
                            (GraphOptCluster) innerGetV.getCluster(),
                            innerGetV.getHints(),
                            input,
                            innerGetV.getOpt(),
                            innerGetV.getTableConfig(),
                            aliasName,
                            innerGetV.getStartAlias());
            newGetV.setFilters(innerGetV.getFilters());
        }
        return new GraphPhysicalGetV(cluster, hints, input, newGetV, physicalOpt);
    }

    public GraphOpt.PhysicalGetVOpt getPhysicalOpt() {
        return this.physicalOpt;
    }

    public AliasNameWithId getStartAlias() {
        return fusedGetV.getStartAlias();
    }

    public String getAliasName() {
        return fusedGetV.getAliasName();
    }

    public int getAliasId() {
        return fusedGetV.getAliasId();
    }

    public GraphLogicalGetV getFusedGetV() {
        return fusedGetV;
    }

    public @Nullable ImmutableList<RexNode> getFilters() {
        return fusedGetV.getFilters();
    }

    @Override
    public List<RelNode> getInputs() {
        return this.input == null ? ImmutableList.of() : ImmutableList.of(this.input);
    }

    @Override
    public RelDataType deriveRowType() {
        return fusedGetV.getRowType();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.itemIf("input", input, !Objects.isNull(input))
                .item("tableConfig", fusedGetV.tableConfig)
                .item("alias", AliasInference.SIMPLE_NAME(fusedGetV.getAliasName()))
                .itemIf(
                        "aliasId",
                        getAliasId(),
                        pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
                .itemIf(
                        "startAlias",
                        fusedGetV.getStartAlias().getAliasName(),
                        fusedGetV.getStartAlias().getAliasName() != AliasInference.DEFAULT_NAME)
                .itemIf(
                        "startAliasId",
                        fusedGetV.getStartAlias().getAliasId(),
                        pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
                .itemIf(
                        "fusedFilter",
                        fusedGetV.getFilters(),
                        !ObjectUtils.isEmpty(fusedGetV.getFilters()))
                .item("opt", fusedGetV.getOpt())
                .item("physicalOpt", getPhysicalOpt());
    }

    @Override
    public GraphPhysicalGetV copy(RelTraitSet traitSet, List<RelNode> inputs) {
        GraphPhysicalGetV copy =
                new GraphPhysicalGetV(
                        getCluster(),
                        fusedGetV.getHints(),
                        inputs.get(0),
                        fusedGetV,
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

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return fusedGetV != null ? fusedGetV.estimateRowCount(mq) : super.estimateRowCount(mq);
    }
}
