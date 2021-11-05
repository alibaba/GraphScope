package com.alibaba.graphscope.common.jna.type;

import com.sun.jna.Structure;

@Structure.FieldOrder({"tag", "property"})
public class FfiVariable extends Structure {
    public static class ByValue extends FfiVariable implements Structure.ByValue {
    }

    public FfiNameOrId.ByValue tag;
    public FfiProperty.ByValue property;
}
