package com.alibaba.graphscope.common.intermediate.core.clause.type;

import com.alibaba.graphscope.common.intermediate.core.validate.TableOpt;

import org.checkerframework.checker.nullness.qual.Nullable;

public class SourceConfig {
    private TableOpt opt;
    @Nullable private String labelName;
    @Nullable private String alias;
}
