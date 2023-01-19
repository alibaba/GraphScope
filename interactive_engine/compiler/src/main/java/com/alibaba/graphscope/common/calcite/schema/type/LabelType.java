package com.alibaba.graphscope.common.calcite.schema.type;

import jline.internal.Nullable;

/**
 * maintain label for each Entity or Relation: Entity(label), Relation(Label, srcLabel, dstLabel).
 */
public class LabelType {
    private String label;
    private int labelId;
    @Nullable private String srcLabel;
    @Nullable private int srcLabelId;
    @Nullable private String dstLabel;
    @Nullable private int dstLabelId;

    public LabelType label(String label) {
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
}
