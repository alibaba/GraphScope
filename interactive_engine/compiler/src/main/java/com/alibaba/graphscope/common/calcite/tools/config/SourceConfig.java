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

import java.util.Objects;

public class SourceConfig {
    private ScanOpt opt;
    private LabelConfig labels;
    private String alias;

    public SourceConfig() {
        this.opt = ScanOpt.Entity;
        this.alias = Static.HEAD;
        this.labels = LabelConfig.DEFAULT;
    }

    public SourceConfig opt(ScanOpt opt) {
        this.opt = Objects.requireNonNull(opt);
        return this;
    }

    public SourceConfig labels(LabelConfig labels) {
        this.labels = Objects.requireNonNull(labels);
        return this;
    }

    /**
     * @param alias can not be null, use a magic value {@code HEAD} to support `head` in gremlin
     * @return
     */
    public SourceConfig alias(String alias) {
        this.alias = Objects.requireNonNull(alias);
        return this;
    }

    public ScanOpt getOpt() {
        return opt;
    }

    public LabelConfig getLabels() {
        return labels;
    }

    public String getAlias() {
        return alias;
    }
}
