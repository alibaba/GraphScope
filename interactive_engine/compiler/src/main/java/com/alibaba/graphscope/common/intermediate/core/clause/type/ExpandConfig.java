package com.alibaba.graphscope.common.intermediate.core.clause.type;

import org.checkerframework.checker.nullness.qual.Nullable;

public class ExpandConfig {
    private DirectionOpt directionOpt;
    @Nullable private String tableName;
    @Nullable private String alias;
}
