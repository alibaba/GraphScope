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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class GetVConfig {
    private final GraphOpt.GetV opt;
    private final LabelConfig labels;
    @Nullable private final String alias;
    @Nullable private final String startAlias;

    public GetVConfig(GraphOpt.GetV opt) {
        this(opt, LabelConfig.DEFAULT, null);
    }

    public GetVConfig(GraphOpt.GetV opt, LabelConfig labels) {
        this(opt, labels, null);
    }

    public GetVConfig(GraphOpt.GetV opt, LabelConfig labels, @Nullable String alias) {
        this(opt, labels, alias, null);
    }

    public GetVConfig(
            GraphOpt.GetV opt,
            LabelConfig labels,
            @Nullable String alias,
            @Nullable String startAlias) {
        this.opt = Objects.requireNonNull(opt);
        this.labels = Objects.requireNonNull(labels);
        this.alias = alias;
        this.startAlias = startAlias;
    }

    public GraphOpt.GetV getOpt() {
        return opt;
    }

    public LabelConfig getLabels() {
        return labels;
    }

    public @Nullable String getAlias() {
        return alias;
    }

    public @Nullable String getStartAlias() {
        return startAlias;
    }
}
