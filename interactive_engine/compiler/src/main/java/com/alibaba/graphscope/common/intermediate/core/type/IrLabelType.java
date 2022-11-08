package com.alibaba.graphscope.common.intermediate.core.type;

import jline.internal.Nullable;

/**
 * maintain labels for each Entity or Relation: Entity(label), Relation(Label, srcLabel, dstLabel).
 */
public class IrLabelType {
    private String label;
    @Nullable private String srcLabel;
    @Nullable private String dstLabel;
}
