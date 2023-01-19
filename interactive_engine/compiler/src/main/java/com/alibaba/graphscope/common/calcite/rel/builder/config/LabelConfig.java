package com.alibaba.graphscope.common.calcite.rel.builder.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * query given labels of a vertex or an edge
 */
public class LabelConfig {
    private List<String> labels;
    private Type type;

    public LabelConfig(Type type) {
        this.type = type;
        this.labels = new ArrayList<>();
    }

    public void addLabel(String label) {
        this.labels.add(label);
    }

    public List<String> getLabels() {
        return Collections.unmodifiableList(labels);
    }

    public Type getType() {
        return type;
    }

    public enum Type {
        SINGLE,
        PARTIAL,
        ALL
    }
}
