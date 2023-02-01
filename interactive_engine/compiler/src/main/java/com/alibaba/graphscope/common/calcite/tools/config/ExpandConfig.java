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

package com.alibaba.graphscope.common.calcite.tools.config;

import com.alibaba.graphscope.common.calcite.util.Static;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

public class ExpandConfig {
    private DirectionOpt directionOpt;
    private LabelConfig labels;
    @Nullable private String alias;

    public ExpandConfig() {
        this.directionOpt = DirectionOpt.OUT;
        this.labels = LabelConfig.DEFAULT;
        this.alias = Static.Alias.DEFAULT_NAME;
    }

    public ExpandConfig opt(DirectionOpt opt) {
        this.directionOpt = Objects.requireNonNull(opt);
        return this;
    }

    public ExpandConfig labels(LabelConfig labels) {
        this.labels = Objects.requireNonNull(labels);
        return this;
    }

    /**
     * @param alias generate a default inner alias if null is given
     * @return
     */
    public ExpandConfig alias(@Nullable String alias) {
        this.alias = (alias == null) ? Static.Alias.DEFAULT_NAME : alias;
        return this;
    }

    public DirectionOpt getOpt() {
        return directionOpt;
    }

    public LabelConfig getLabels() {
        return labels;
    }

    public String getAlias() {
        return alias;
    }
}
