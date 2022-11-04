package com.alibaba.graphscope.common.intermediate.core.type;

import jline.internal.Nullable;

import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;

import java.util.Collections;

/**
 * maintain labels for each Entity and Relation: Entity(label), Relation(Label, srcLabel, dstLabel).
 */
public class IrLabelType extends RelRecordType {
    private String label;
    @Nullable private String srcLabel;
    @Nullable private String dstLabel;

    public IrLabelType() {
        super(StructKind.NONE, Collections.EMPTY_LIST);
    }
}
