package com.alibaba.graphscope.common.intermediate.calcite.type;

import jline.internal.Nullable;

/**
 * maintain labels for each Entity or Relation: Entity(label), Relation(Label, srcLabel, dstLabel).
 */
public class IrLabelType {
    private String label;
    private int labelId;
    @Nullable private String srcLabel;
    @Nullable private int srcLabelId;
    @Nullable private String dstLabel;
    @Nullable private int dstLabelId;
}
