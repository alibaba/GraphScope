package com.alibaba.graphscope.common.jna.type;

import com.alibaba.graphscope.common.jna.IntEnum;

public enum FfiJoinKind implements IntEnum<FfiJoinKind> {
    /// Inner join
    Inner,
    /// Left outer join
    LeftOuter,
    /// Right outer join
    RightOuter,
    /// Full outer join
    FullOuter,
    /// Left semi-join, right alternative can be naturally adapted
    Semi,
    /// Left anti-join, right alternative can be naturally adapted
    Anti,
    /// aka. Cartesian product
    Times;

    @Override
    public int getInt() {
        return this.ordinal();
    }

    @Override
    public FfiJoinKind getEnum(int i) {
        FfiJoinKind opts[] = values();
        if (i < opts.length && i >= 0) {
            return opts[i];
        }
        return null;
    }
}
