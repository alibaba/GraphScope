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

import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;
import com.alibaba.graphscope.common.ir.type.GraphArrayType;
import com.alibaba.graphscope.common.ir.type.GraphPxdElementType;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class GraphLogicalPathExpand extends SingleRel {
    private final RelNode expand;
    private final RelNode getV;

    private final @Nullable RexNode offset;
    private final @Nullable RexNode fetch;

    private final GraphOpt.PathExpandResult resultOpt;
    private final GraphOpt.PathExpandPath pathOpt;

    private final String aliasName;

    private final int aliasId;

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
            @Nullable String aliasName) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.expand = Objects.requireNonNull(expand);
        this.getV = Objects.requireNonNull(getV);
        this.offset = offset;
        this.fetch = fetch;
        this.rowType =
                new GraphArrayType(
                        new GraphPxdElementType(this.expand.getRowType(), this.getV.getRowType()));
        this.resultOpt = resultOpt;
        this.pathOpt = pathOpt;
        this.aliasName =
                AliasInference.inferDefault(
                        aliasName, AliasInference.getUniqueAliasList(input, true));
        this.aliasId = cluster.getIdGenerator().generate(this.aliasName, input);
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
            String aliasName) {
        return new GraphLogicalPathExpand(
                cluster, hints, input, expand, getV, offset, fetch, resultOpt, pathOpt, aliasName);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("expand", RelOptUtil.toString(expand))
                .item("getV", RelOptUtil.toString(getV))
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null)
                .item("path_opt", getPathOpt())
                .item("result_opt", getResultOpt())
                .item("alias", getAliasName());
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
}
