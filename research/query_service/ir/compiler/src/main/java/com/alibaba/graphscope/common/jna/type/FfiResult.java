package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.sun.jna.Structure;

@Structure.FieldOrder({"code", "msg"})
public class FfiResult extends Structure {
    public FfiResult() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiResult implements Structure.ByValue {}

    public ResultCode code;
    public String msg;
}
