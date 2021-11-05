package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

@Structure.FieldOrder({"dataType", "bool", "int32", "int64", "float64", "cstr", "raw"})
public class FfiConst extends Structure {
    public FfiConst() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiConst implements Structure.ByValue {
    }

    public FfiDataType dataType;
    public boolean bool;
    public int int32;
    public long int64;
    public double float64;
    public String cstr;
    public Pointer raw;
}
