package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.sun.jna.Structure;

@Structure.FieldOrder({"opt", "key"})
public class FfiProperty extends Structure {
    public FfiProperty() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiProperty implements Structure.ByValue {
    }

    public FfiPropertyOpt opt;
    public FfiNameOrId.ByValue key;
}

