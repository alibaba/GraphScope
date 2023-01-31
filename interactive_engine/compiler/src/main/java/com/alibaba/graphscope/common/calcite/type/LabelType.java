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

package com.alibaba.graphscope.common.calcite.type;

import jline.internal.Nullable;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Maintain label for each Entity or Relation: Entity(label), Relation(Label, srcLabel, dstLabel).
 */
public class LabelType {
    public static LabelType DEFAULT = new LabelType();

    private String label;
    private int labelId;
    @Nullable private String srcLabel;
    @Nullable private int srcLabelId;
    @Nullable private String dstLabel;
    @Nullable private int dstLabelId;

    public LabelType() {
        this.label = StringUtils.EMPTY;
        this.labelId = -1;
    }

    public LabelType label(String label) {
        Objects.requireNonNull(label);
        this.label = label;
        return this;
    }

    public LabelType labelId(int labelId) {
        this.labelId = labelId;
        return this;
    }

    public LabelType srcLabel(String srcLabel) {
        this.srcLabel = srcLabel;
        return this;
    }

    public LabelType srcLabelId(int srcLabelId) {
        this.srcLabelId = srcLabelId;
        return this;
    }

    public LabelType dstLabel(String dstLabel) {
        this.dstLabel = dstLabel;
        return this;
    }

    public LabelType dstLabelId(int dstLabelId) {
        this.dstLabelId = dstLabelId;
        return this;
    }

    public String getLabel() {
        return label;
    }

    public int getLabelId() {
        return labelId;
    }

    public String getSrcLabel() {
        return srcLabel;
    }

    public int getSrcLabelId() {
        return srcLabelId;
    }

    public String getDstLabel() {
        return dstLabel;
    }

    public int getDstLabelId() {
        return dstLabelId;
    }
}
