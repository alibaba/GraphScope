package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiKeyType implements IntEnum<FfiKeyType> {
    Entity,
    Relation,
    Column;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiKeyType getEnum(int i) {
        FfiKeyType opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
