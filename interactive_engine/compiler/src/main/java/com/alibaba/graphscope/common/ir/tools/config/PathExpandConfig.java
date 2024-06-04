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

package com.alibaba.graphscope.common.ir.tools.config;

import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalExpand;
import com.alibaba.graphscope.common.ir.rel.graph.GraphLogicalGetV;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * build {@code PathExpandConfig} (used to build {@code PathExpandOperator}) from user given configs
 */
public class PathExpandConfig {
    private final RelNode expand;
    private final RelNode getV;

    private final int offset;
    private final int fetch;

    private final GraphOpt.PathExpandPath pathOpt;
    private final GraphOpt.PathExpandResult resultOpt;
    private final @Nullable RexNode untilCondition;

    @Nullable private final String alias;
    @Nullable private final String startAlias;

    protected PathExpandConfig(
            RelNode expand,
            RelNode getV,
            int offset,
            int fetch,
            GraphOpt.PathExpandResult resultOpt,
            GraphOpt.PathExpandPath pathOpt,
            @Nullable RexNode untilCondition,
            @Nullable String alias,
            @Nullable String startAlias) {
        this.expand = Objects.requireNonNull(expand);
        this.getV = Objects.requireNonNull(getV);
        this.offset = offset;
        this.fetch = fetch;
        this.resultOpt = resultOpt;
        this.pathOpt = pathOpt;
        this.untilCondition = untilCondition;
        this.alias = alias;
        this.startAlias = startAlias;
    }

    public static Builder newBuilder(GraphBuilder innerBuilder) {
        return new Builder(innerBuilder);
    }

    public @Nullable String getAlias() {
        return alias;
    }

    public @Nullable String getStartAlias() {
        return startAlias;
    }

    public GraphOpt.PathExpandPath getPathOpt() {
        return pathOpt;
    }

    public GraphOpt.PathExpandResult getResultOpt() {
        return resultOpt;
    }

    public int getOffset() {
        return offset;
    }

    public int getFetch() {
        return fetch;
    }

    public RelNode getExpand() {
        return expand;
    }

    public RelNode getGetV() {
        return getV;
    }

    public @Nullable RexNode getUntilCondition() {
        return untilCondition;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PathExpandConfig{");
        builder.append("expand=" + expand.explain());
        builder.append(", getV=" + getV.explain());
        builder.append(", offset=" + offset);
        builder.append(", fetch=" + fetch);
        builder.append(", pathOpt=" + pathOpt);
        builder.append(", resultOpt=" + resultOpt);
        if (untilCondition != null) {
            builder.append(", untilCondition=" + untilCondition);
        }
        builder.append(", alias='" + alias + '\'');
        builder.append("}");
        return builder.toString();
    }

    public static final class Builder extends GraphBuilder {
        private RelNode expand;
        private RelNode getV;

        private int offset;
        private int fetch;

        private GraphOpt.PathExpandPath pathOpt;
        private GraphOpt.PathExpandResult resultOpt;

        private @Nullable RexNode untilCondition;

        @Nullable private String alias;
        @Nullable private String startAlias;

        protected Builder(GraphBuilder parentBuilder) {
            super(
                    parentBuilder.getContext(),
                    (GraphOptCluster) parentBuilder.getCluster(),
                    parentBuilder.getRelOptSchema());
            if (parentBuilder.size() > 0) {
                this.push(parentBuilder.peek());
            }
            this.pathOpt = GraphOpt.PathExpandPath.ARBITRARY;
            this.resultOpt = GraphOpt.PathExpandResult.END_V;
        }

        public Builder expand(ExpandConfig config) {
            if (this.getV == null && this.expand == null) {
                GraphLogicalExpand expandRel = (GraphLogicalExpand) super.expand(config).build();
                this.expand =
                        GraphLogicalExpand.create(
                                (GraphOptCluster) expandRel.getCluster(),
                                ImmutableList.of(),
                                null,
                                expandRel.getOpt(),
                                expandRel.getTableConfig(),
                                AliasInference.DEFAULT_NAME,
                                AliasNameWithId.DEFAULT);
                push(this.expand);
            }
            return this;
        }

        public Builder getV(GetVConfig config) {
            if (this.expand != null && this.getV == null) {
                GraphLogicalGetV getVRel = (GraphLogicalGetV) super.getV(config).build();
                this.getV =
                        GraphLogicalGetV.create(
                                (GraphOptCluster) getVRel.getCluster(),
                                ImmutableList.of(),
                                null,
                                getVRel.getOpt(),
                                getVRel.getTableConfig(),
                                AliasInference.DEFAULT_NAME,
                                AliasNameWithId.DEFAULT);
                push(this.getV);
            }
            return this;
        }

        public Builder filter(RexNode... conjunctions) {
            return (Builder) super.filter(conjunctions);
        }

        public Builder untilCondition(@Nullable RexNode untilCondition) {
            this.untilCondition = untilCondition;
            return this;
        }

        public Builder range(int offset, int fetch) {
            this.offset = offset;
            this.fetch = fetch;
            return this;
        }

        public Builder pathOpt(GraphOpt.PathExpandPath pathOpt) {
            this.pathOpt = pathOpt;
            return this;
        }

        public Builder resultOpt(GraphOpt.PathExpandResult resultOpt) {
            this.resultOpt = resultOpt;
            return this;
        }

        public Builder alias(@Nullable String alias) {
            this.alias = alias;
            return this;
        }

        public Builder startAlias(@Nullable String startAlias) {
            this.startAlias = startAlias;
            return this;
        }

        public PathExpandConfig buildConfig() {
            return new PathExpandConfig(
                    expand,
                    getV,
                    offset,
                    fetch,
                    resultOpt,
                    pathOpt,
                    untilCondition,
                    alias,
                    startAlias);
        }
    }
}
