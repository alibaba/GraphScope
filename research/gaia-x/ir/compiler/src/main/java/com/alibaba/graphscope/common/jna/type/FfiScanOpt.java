package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiScanOpt implements IntEnum<FfiScanOpt> {
    Vertex,
    Edge,
    Table;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiScanOpt getEnum(int i) {
        FfiScanOpt opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
