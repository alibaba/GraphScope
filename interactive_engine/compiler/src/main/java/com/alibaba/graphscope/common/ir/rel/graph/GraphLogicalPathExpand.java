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
import com.alibaba.graphscope.common.ir.type.GraphPathType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class GraphLogicalPathExpand extends SingleRel {
    private final RelNode expand;
    private final RelNode getV;

    // fuse expand with getV to a single operator
    private final RelNode fused;

    private final @Nullable RexNode offset;
    private final @Nullable RexNode fetch;

    private final GraphOpt.PathExpandResult resultOpt;
    private final GraphOpt.PathExpandPath pathOpt;

    private final String aliasName;

    private final int aliasId;

    private final AliasNameWithId startAlias;

    protected GraphLogicalPathExpand(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            RelNode input,
            RelNode expand,
            RelNode getV,
            @Nullable RexNode offset,
            @Nullable RexNode fetch,
            GraphOpt.PathExpandResult resultOpt,
            GraphOpt.PathExpandPath pathOpt,
            @Nullable String aliasName,
            AliasNameWithId startAlias) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.expand = Objects.requireNonNull(expand);
        this.getV = Objects.requireNonNull(getV);
        this.fused = null;
        this.offset = offset;
        this.fetch = fetch;
        this.resultOpt = resultOpt;
        this.pathOpt = pathOpt;
        this.aliasName =
                AliasInference.inferDefault(
                        aliasName, AliasInference.getUniqueAliasList(input, true));
        this.aliasId = cluster.getIdGenerator().generate(this.aliasName);
        this.startAlias = Objects.requireNonNull(startAlias);
    }

    protected GraphLogicalPathExpand(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            RelNode input,
            RelNode fused,
            @Nullable RexNode offset,
            @Nullable RexNode fetch,
            GraphOpt.PathExpandResult resultOpt,
            GraphOpt.PathExpandPath pathOpt,
            @Nullable String aliasName,
            AliasNameWithId startAlias) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.expand = null;
        this.getV = null;
        this.fused = Objects.requireNonNull(fused);
        this.offset = offset;
        this.fetch = fetch;
        this.resultOpt = resultOpt;
        this.pathOpt = pathOpt;
        this.aliasName =
                AliasInference.inferDefault(
                        aliasName, AliasInference.getUniqueAliasList(input, true));
        this.aliasId = cluster.getIdGenerator().generate(this.aliasName);
        this.startAlias = Objects.requireNonNull(startAlias);
    }

    public static GraphLogicalPathExpand create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            RelNode expand,
            RelNode getV,
            @Nullable RexNode offset,
            @Nullable RexNode fetch,
            GraphOpt.PathExpandResult resultOpt,
            GraphOpt.PathExpandPath pathOpt,
            String aliasName,
            AliasNameWithId startAlias) {
        return new GraphLogicalPathExpand(
                cluster,
                hints,
                input,
                expand,
                getV,
                offset,
                fetch,
                resultOpt,
                pathOpt,
                aliasName,
                startAlias);
    }

    public static GraphLogicalPathExpand create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            RelNode fused,
            @Nullable RexNode offset,
            @Nullable RexNode fetch,
            GraphOpt.PathExpandResult resultOpt,
            GraphOpt.PathExpandPath pathOpt,
            String aliasName,
            AliasNameWithId startAlias) {
        Preconditions.checkArgument(
                resultOpt != GraphOpt.PathExpandResult.ALL_V_E,
                "can not fuse expand with getV if result opt is set to " + resultOpt.name());
        return new GraphLogicalPathExpand(
                cluster,
                hints,
                input,
                fused,
                offset,
                fetch,
                resultOpt,
                pathOpt,
                aliasName,
                startAlias);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("expand", RelOptUtil.toString(expand), expand != null)
                .itemIf("getV", RelOptUtil.toString(getV), getV != null)
                .itemIf("fused", RelOptUtil.toString(fused), fused != null)
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null)
                .item("path_opt", getPathOpt())
                .item("result_opt", getResultOpt())
                .item("alias", AliasInference.SIMPLE_NAME(getAliasName()))
                .itemIf(
                        "start_alias",
                        startAlias.getAliasName(),
                        startAlias.getAliasName() != AliasInference.DEFAULT_NAME);
    }

    public String getAliasName() {
        return this.aliasName;
    }

    public int getAliasId() {
        return this.aliasId;
    }

    public GraphOpt.PathExpandPath getPathOpt() {
        return this.pathOpt;
    }

    public GraphOpt.PathExpandResult getResultOpt() {
        return this.resultOpt;
    }

    public RelNode getExpand() {
        return expand;
    }

    public RelNode getGetV() {
        return getV;
    }

    public @Nullable RexNode getOffset() {
        return offset;
    }

    public @Nullable RexNode getFetch() {
        return fetch;
    }

    public AliasNameWithId getStartAlias() {
        return startAlias;
    }

    @Override
    protected RelDataType deriveRowType() {
        return new RelRecordType(
                ImmutableList.of(
                        new RelDataTypeFieldImpl(
                                getAliasName(),
                                getAliasId(),
                                new GraphPathType(getElementType()))));
    }

    private GraphPathType.ElementType getElementType() {
        switch (resultOpt) {
            case ALL_V:
            case END_V:
                RelNode getV = this.fused != null ? this.fused : this.getV;
                ObjectUtils.requireNonEmpty(
                        getV.getRowType().getFieldList(),
                        "data type of getV operator should have at least one column field");
                return new GraphPathType.ElementType(
                        getV.getRowType().getFieldList().get(0).getType());
            case ALL_V_E:
            default:
                ObjectUtils.requireNonEmpty(
                        this.expand.getRowType().getFieldList(),
                        "data type of expand operator should have at least one column field");
                ObjectUtils.requireNonEmpty(
                        this.getV.getRowType().getFieldList(),
                        "data type of getV operator should have at least one column field");
                return new GraphPathType.ElementType(
                        this.expand.getRowType().getFieldList().get(0).getType(),
                        this.getV.getRowType().getFieldList().get(0).getType());
        }
    }

    @Override
    public GraphLogicalPathExpand copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (this.fused != null) {
            return new GraphLogicalPathExpand(
                    (GraphOptCluster) getCluster(),
                    ImmutableList.of(),
                    inputs.get(0),
                    this.fused,
                    getOffset(),
                    getFetch(),
                    getResultOpt(),
                    getPathOpt(),
                    getAliasName(),
                    getStartAlias());
        } else {
            return new GraphLogicalPathExpand(
                    (GraphOptCluster) getCluster(),
                    ImmutableList.of(),
                    inputs.get(0),
                    this.expand,
                    this.getV,
                    getOffset(),
                    getFetch(),
                    getResultOpt(),
                    getPathOpt(),
                    getAliasName(),
                    getStartAlias());
        }
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphShuttle) {
            return ((GraphShuttle) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }
}
