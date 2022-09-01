package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum ResultOpt implements IntEnum<ResultOpt> {
    EndV,
    AllV;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public ResultOpt getEnum(int i) {
        ResultOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
