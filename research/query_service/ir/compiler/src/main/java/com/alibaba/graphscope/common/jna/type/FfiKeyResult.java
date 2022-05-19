package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.sun.jna.Structure;

@Structure.FieldOrder({"keyName", "error"})
public class FfiKeyResult extends Structure {
    public FfiKeyResult() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiKeyResult implements Structure.ByValue {}

    public String keyName;
    public FfiError.ByValue error;
}
