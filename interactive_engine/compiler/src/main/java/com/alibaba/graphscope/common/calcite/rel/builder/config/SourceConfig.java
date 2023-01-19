package com.alibaba.graphscope.common.calcite.rel.builder.config;

import org.checkerframework.checker.nullness.qual.Nullable;

public class SourceConfig {
    private ScanOpt opt;
    private LabelConfig labels;
    // can be null, to support `head` tag in gremlin
    @Nullable private String alias;

    public SourceConfig opt(ScanOpt opt) {
        this.opt = opt;
        return this;
    }

    public SourceConfig labels(LabelConfig labels) {
        this.labels = labels;
        return this;
    }

    public SourceConfig alias(@Nullable String alias) {
        this.alias = alias;
        return this;
    }

    public ScanOpt getOpt() {
        return opt;
    }

    public LabelConfig getLabels() {
        return labels;
    }

    public String getAlias() {
        return alias;
    }
}
