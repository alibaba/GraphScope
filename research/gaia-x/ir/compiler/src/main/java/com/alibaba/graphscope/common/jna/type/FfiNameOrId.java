package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IrTypeMapper;
import com.sun.jna.Structure;

@Structure.FieldOrder({"opt", "name", "nameId"})
public class FfiNameOrId extends Structure {
    private FfiNameOrId() {
        super(IrTypeMapper.INSTANCE);
    }

    public static class ByValue extends FfiNameOrId implements Structure.ByValue {
    }

    public FfiNameIdOpt opt;
    public String name;
    public int nameId;

    public static FfiNameOrId create() {
        return new FfiNameOrId();
    }

    public FfiNameOrId name(String name) {
        this.opt = FfiNameIdOpt.Name;
        this.name = name;
        return this;
    }

    public FfiNameOrId nameId(int nameId) {
        this.opt = FfiNameIdOpt.Id;
        this.nameId = nameId;
        return this;
    }
}
