package com.alibaba.graphscope.common.calcite.rel.builder.config;

import org.checkerframework.checker.nullness.qual.Nullable;

public class ExpandConfig {
    private DirectionOpt directionOpt;
    private LabelConfig labels;
    @Nullable private String alias;
}
