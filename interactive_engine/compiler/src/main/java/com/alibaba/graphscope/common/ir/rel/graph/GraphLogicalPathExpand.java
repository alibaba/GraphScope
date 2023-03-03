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
import org.apache.commons.lang3.ObjectUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

public class GraphLogicalPathExpand extends SingleRel {
    private final RelNode expand;
    private final RelNode getV;

    private final @Nullable RexNode offset;
    private final @Nullable RexNode fetch;

    private final List<RelHint> hints;

    protected GraphLogicalPathExpand(
            GraphOptCluster cluster,
            @Nullable List<RelHint> hints,
            RelNode input,
            RelNode expand,
            RelNode getV,
            @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        super(cluster, RelTraitSet.createEmpty(), input);
        this.hints = hints;
        this.expand = Objects.requireNonNull(expand);
        this.getV = Objects.requireNonNull(getV);
        this.offset = offset;
        this.fetch = fetch;
        this.rowType =
                new GraphArrayType(
                        new GraphPxdElementType(this.expand.getRowType(), this.getV.getRowType()));
    }

    public static GraphLogicalPathExpand create(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            RelNode expand,
            RelNode getV,
            @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        return new GraphLogicalPathExpand(cluster, hints, input, expand, getV, offset, fetch);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("expand", RelOptUtil.toString(expand))
                .item("getV", RelOptUtil.toString(getV))
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null)
                .item("path_opt", pathOpt())
                .item("result_opt", resultOpt())
                .item("alias", getAliasName());
    }

    public String getAliasName() {
        Objects.requireNonNull(hints);
        if (hints.size() < 2) {
            throw new IllegalArgumentException(
                    "should have put alias config in the index 1 of the hints list");
        }
        RelHint aliasHint = hints.get(1);
        Objects.requireNonNull(aliasHint.kvOptions);
        String aliasName = aliasHint.kvOptions.get("name");
        Objects.requireNonNull(aliasName);
        return aliasName;
    }

    public int getAliasId() {
        Objects.requireNonNull(hints);
        if (hints.size() < 2) {
            throw new IllegalArgumentException(
                    "should have put alias config in the index 1 of the hints list");
        }
        RelHint aliasHint = hints.get(1);
        Objects.requireNonNull(aliasHint.kvOptions);
        String aliasId = aliasHint.kvOptions.get("id");
        Objects.requireNonNull(aliasId);
        return Integer.valueOf(aliasId);
    }

    public GraphOpt.PathExpandPath pathOpt() {
        ObjectUtils.requireNonEmpty(hints);
        RelHint optHint = hints.get(0);
        ObjectUtils.requireNonEmpty(optHint.kvOptions);
        return GraphOpt.PathExpandPath.valueOf(optHint.kvOptions.get("path"));
    }

    public GraphOpt.PathExpandResult resultOpt() {
        ObjectUtils.requireNonEmpty(hints);
        RelHint optHint = hints.get(0);
        ObjectUtils.requireNonEmpty(optHint.kvOptions);
        return GraphOpt.PathExpandResult.valueOf(optHint.kvOptions.get("result"));
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
