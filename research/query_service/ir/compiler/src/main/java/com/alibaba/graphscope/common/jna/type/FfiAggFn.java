package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

@Structure.FieldOrder({"vars", "aggregate", "alias"})
public class FfiAggFn extends Structure {
    public FfiAggFn() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiAggFn implements Structure.ByValue {
    }

    public Pointer vars;
    public FfiAggOpt aggregate;
    public FfiAlias.ByValue alias;
}
