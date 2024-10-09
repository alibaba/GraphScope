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
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class GraphPhysicalExpand extends SingleRel {
    private final GraphOpt.PhysicalExpandOpt physicalOpt;
    private final GraphLogicalExpand fusedExpand;
    private final GraphLogicalGetV fusedGetV;
    private final String aliasName;
    private final int aliasId;

    private final boolean optional;

    protected GraphPhysicalExpand(
            RelOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalExpand fusedExpand,
            GraphLogicalGetV fusedGetV,
            GraphOpt.PhysicalExpandOpt physicalOpt,
            String aliasName,
            boolean optional) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.physicalOpt = physicalOpt;
        this.fusedExpand = fusedExpand;
        this.fusedGetV = fusedGetV;
        this.aliasName =
                AliasInference.inferDefault(
                        aliasName, AliasInference.getUniqueAliasList(input, true));
        this.aliasId = ((GraphOptCluster) cluster).getIdGenerator().generate(this.aliasName);
        this.optional = optional;
    }

    public static GraphPhysicalExpand create(
            RelOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphLogicalExpand fusedExpand,
            GraphLogicalGetV fusedGetV,
            GraphOpt.PhysicalExpandOpt physicalOpt,
            String aliasName) {
        GraphLogicalGetV newGetV = null;
        if (fusedGetV != null) {
            // if fused to output vertices, build a new getV if a new aliasName is given, to make
            // sure the derived row type is correct (which is derived by getV)
            if (fusedGetV.getAliasName().equals(aliasName)) {
                newGetV = fusedGetV;
            } else {
                newGetV =
                        GraphLogicalGetV.create(
                                (GraphOptCluster) fusedGetV.getCluster(),
                                fusedGetV.getHints(),
                                input,
                                fusedGetV.getOpt(),
                                fusedGetV.getTableConfig(),
                                aliasName,
                                fusedGetV.getStartAlias());
                if (ObjectUtils.isNotEmpty(fusedGetV.getFilters())) {
                    // should not have filters, as it would built as a new PhysicalGetV
                    newGetV.setFilters(fusedGetV.getFilters());
                }
            }
        }
        return new GraphPhysicalExpand(
                cluster,
                hints,
                input,
                fusedExpand,
                newGetV,
                physicalOpt,
                aliasName,
                fusedExpand.isOptional());
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
        return aliasName;
    }

    public int getAliasId() {
        return aliasId;
    }

    public @Nullable ImmutableList<RexNode> getFilters() {
        return fusedExpand.getFilters();
    }

    public boolean isOptional() {
        return optional;
    }

    @Override
    public RelDataType deriveRowType() {
        switch (physicalOpt) {
            case EDGE:
                return fusedExpand.getRowType();
            case DEGREE:
                {
                    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
                    RelDataTypeField field =
                            new RelDataTypeFieldImpl(
                                    aliasName,
                                    aliasId,
                                    typeFactory.createSqlType(SqlTypeName.BIGINT));
                    return new RelRecordType(ImmutableList.of(field));
                }
            case VERTEX:
            default:
                return fusedGetV.getRowType();
        }
    }

    @Override
    public List<RelNode> getInputs() {
        return this.input == null ? ImmutableList.of() : ImmutableList.of(this.input);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.itemIf("input", input, !Objects.isNull(input))
                .item("tableConfig", fusedExpand.explainTableConfig())
                .item("alias", AliasInference.SIMPLE_NAME(getAliasName()))
                .itemIf(
                        "aliasId",
                        getAliasId(),
                        pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
                .itemIf(
                        "startAlias",
                        fusedExpand.getStartAlias().getAliasName(),
                        fusedExpand.getStartAlias().getAliasName() != AliasInference.DEFAULT_NAME)
                .itemIf(
                        "startAliasId",
                        fusedExpand.getStartAlias().getAliasId(),
                        pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
                .itemIf(
                        "fusedFilter",
                        fusedExpand.getFilters(),
                        !ObjectUtils.isEmpty(fusedExpand.getFilters()))
                .item("opt", fusedExpand.getOpt())
                .item("physicalOpt", getPhysicalOpt())
                .itemIf("optional", optional, optional);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }

    @Override
    public GraphPhysicalExpand copy(RelTraitSet traitSet, List<RelNode> inputs) {
        GraphPhysicalExpand copy =
                new GraphPhysicalExpand(
                        getCluster(),
                        fusedGetV.getHints(),
                        inputs.get(0),
                        fusedExpand,
                        fusedGetV,
                        getPhysicalOpt(),
                        aliasName,
                        optional);
        return copy;
    }

    public GraphLogicalGetV getFusedGetV() {
        return fusedGetV;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return fusedExpand != null ? fusedExpand.estimateRowCount(mq) : super.estimateRowCount(mq);
    }
}
