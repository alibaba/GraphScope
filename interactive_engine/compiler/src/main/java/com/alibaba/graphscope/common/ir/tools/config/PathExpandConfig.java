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
import com.alibaba.graphscope.common.ir.tools.AliasInference;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.jna.type.PathOpt;
import com.alibaba.graphscope.common.jna.type.ResultOpt;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * build {@code PathExpandConfig} (used to build {@code PathExpandOperator}) from user given configs
 */
public class PathExpandConfig {
    private final RelNode expand;
    private final RelNode getV;

    private final int offset;
    private final int fetch;

    private final PathOpt pathOpt;
    private final ResultOpt resultOpt;

    @Nullable private final String alias;

    protected PathExpandConfig(
            RelNode expand,
            RelNode getV,
            int offset,
            int fetch,
            ResultOpt resultOpt,
            PathOpt pathOpt,
            @Nullable String alias) {
        this.expand = Objects.requireNonNull(expand);
        this.getV = Objects.requireNonNull(getV);
        this.offset = offset;
        this.fetch = fetch;
        this.resultOpt = resultOpt;
        this.pathOpt = pathOpt;
        this.alias = alias;
    }

    public static Builder newBuilder(GraphBuilder innerBuilder) {
        return new Builder(innerBuilder);
    }

    public @Nullable String getAlias() {
        return alias;
    }

    public PathOpt getPathOpt() {
        return pathOpt;
    }

    public ResultOpt getResultOpt() {
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

    public static final class Builder {
        private final GraphBuilder innerBuilder;

        private RelNode expand;
        private RelNode getV;

        private int offset;
        private int fetch;

        private PathOpt pathOpt;
        private ResultOpt resultOpt;

        @Nullable private String alias;

        protected Builder(GraphBuilder innerBuilder) {
            this.innerBuilder = innerBuilder;
            this.pathOpt = PathOpt.Arbitrary;
            this.resultOpt = ResultOpt.EndV;
        }

        public Builder expand(ExpandConfig config) {
            if (this.getV == null && this.expand == null) {
                this.expand =
                        GraphLogicalExpand.create(
                                (GraphOptCluster) innerBuilder.getCluster(),
                                innerBuilder.getHints(
                                        config.getOpt().name(),
                                        AliasInference.DEFAULT_NAME,
                                        AliasInference.DEFAULT_ID),
                                null,
                                innerBuilder.getTableConfig(
                                        config.getLabels(), GraphOpt.Source.EDGE));
            }
            return this;
        }

        public Builder getV(GetVConfig config) {
            if (this.expand != null && this.getV == null) {
                this.getV =
                        GraphLogicalGetV.create(
                                (GraphOptCluster) innerBuilder.getCluster(),
                                innerBuilder.getHints(
                                        config.getOpt().name(),
                                        AliasInference.DEFAULT_NAME,
                                        AliasInference.DEFAULT_ID),
                                null,
                                innerBuilder.getTableConfig(
                                        config.getLabels(), GraphOpt.Source.VERTEX));
            }
            return this;
        }

        public Builder filter(List<RexNode> conjunctions) {
            if (this.getV != null) {
                this.getV = innerBuilder.push(this.getV).filter(conjunctions).build();
            } else if (this.expand != null) {
                this.expand = innerBuilder.push(this.expand).filter(conjunctions).build();
            }
            return this;
        }

        public Builder range(int offset, int fetch) {
            this.offset = offset;
            this.fetch = fetch;
            return this;
        }

        public Builder pathOpt(PathOpt pathOpt) {
            this.pathOpt = pathOpt;
            return this;
        }

        public Builder resultOpt(ResultOpt resultOpt) {
            this.resultOpt = resultOpt;
            return this;
        }

        public Builder alias(@Nullable String alias) {
            this.alias = alias;
            return this;
        }

        public PathExpandConfig build() {
            return new PathExpandConfig(expand, getV, offset, fetch, resultOpt, pathOpt, alias);
        }
    }
}
