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

package com.alibaba.graphscope.common.ir.type;

import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * Maintain label for each Entity or Relation: Entity(label), Relation(Label, srcLabel, dstLabel).
 */
public class GraphLabelType {
    public static GraphLabelType DEFAULT = new GraphLabelType();

    private String label;
    private Integer labelId;
    @Nullable private String srcLabel;
    @Nullable private Integer srcLabelId;
    @Nullable private String dstLabel;
    @Nullable private Integer dstLabelId;

    public GraphLabelType() {
        this.label = StringUtils.EMPTY;
        this.labelId = -1;
    }

    public GraphLabelType label(String label) {
        Objects.requireNonNull(label);
        this.label = label;
        return this;
    }

    public GraphLabelType labelId(int labelId) {
        this.labelId = labelId;
        return this;
    }

    public GraphLabelType srcLabel(String srcLabel) {
        this.srcLabel = srcLabel;
        return this;
    }

    public GraphLabelType srcLabelId(int srcLabelId) {
        this.srcLabelId = srcLabelId;
        return this;
    }

    public GraphLabelType dstLabel(String dstLabel) {
        this.dstLabel = dstLabel;
        return this;
    }

    public GraphLabelType dstLabelId(int dstLabelId) {
        this.dstLabelId = dstLabelId;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public Integer getLabelId() {
        return labelId;
    }

    public @Nullable String getSrcLabel() {
        return srcLabel;
    }

    public @Nullable Integer getSrcLabelId() {
        return srcLabelId;
    }

    public @Nullable String getDstLabel() {
        return dstLabel;
    }

    public @Nullable Integer getDstLabelId() {
        return dstLabelId;
    }
}
