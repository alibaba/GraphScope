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

public class SourceConfig {
    private final GraphOpt.Source opt;
    private final LabelConfig labels;
    @Nullable private final String alias;

    public SourceConfig(GraphOpt.Source opt) {
        this(opt, LabelConfig.DEFAULT, null);
    }

    public SourceConfig(GraphOpt.Source opt, LabelConfig labels) {
        this(opt, labels, null);
    }

    public SourceConfig(GraphOpt.Source opt, LabelConfig labels, @Nullable String alias) {
        this.opt = Objects.requireNonNull(opt);
        this.labels = Objects.requireNonNull(labels);
        this.alias = alias;
    }

    public GraphOpt.Source getOpt() {
        return opt;
    }

    public LabelConfig getLabels() {
        return labels;
    }

    public @Nullable String getAlias() {
        return alias;
    }
}
