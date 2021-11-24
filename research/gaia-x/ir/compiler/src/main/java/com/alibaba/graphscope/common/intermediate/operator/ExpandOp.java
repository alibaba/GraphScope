package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class ExpandOp extends InterOpBase {
    public ExpandOp() {
        super();
        this.edgeOpt = Optional.empty();
        this.direction = Optional.empty();
        this.labels = Optional.empty();
        this.properties = Optional.empty();
        this.predicate = Optional.empty();
        this.limit = Optional.empty();
    }

    // out or outE
    private Optional<OpArg> edgeOpt;

    // in/out/both
    private Optional<OpArg> direction;

    // filter edge by labels
    private Optional<OpArg> labels;

    // filter edge by properties
    private Optional<OpArg> properties;

    // filter edge by predicates
    private Optional<OpArg> predicate;

    private Optional<OpArg> limit;

    public Optional<OpArg> getDirection() {
        return direction;
    }

    public Optional<OpArg> getLabels() {
        return labels;
    }

    public Optional<OpArg> getProperties() {
        return properties;
    }

    public Optional<OpArg> getPredicate() {
        return predicate;
    }

    public Optional<OpArg> getLimit() {
        return limit;
    }

    public void setDirection(OpArg direction) {
        this.direction = Optional.of(direction);
    }

    public void setLabels(OpArg labels) {
        this.labels = Optional.of(labels);
    }

    public void setProperties(OpArg properties) {
        this.properties = Optional.of(properties);
    }

    public void setPredicate(OpArg predicate) {
        this.predicate = Optional.of(predicate);
    }

    public void setLimit(OpArg limit) {
        this.limit = Optional.of(limit);
    }

    public Optional<OpArg> getEdgeOpt() {
        return edgeOpt;
    }

    public void setEdgeOpt(OpArg edgeOpt) {
        this.edgeOpt = Optional.of(edgeOpt);
    }
}
