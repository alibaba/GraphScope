package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class PathExpandOp extends ExpandOp {
    private Optional<OpArg> lower;

    private Optional<OpArg> upper;

    public PathExpandOp() {
        super();
        lower = Optional.empty();
        upper = Optional.empty();
    }

    public PathExpandOp(ExpandOp other) {
        this();
        if (other.getAlias().isPresent()) {
            setAlias(other.getAlias().get());
        }
        if (other.getIsEdge().isPresent()) {
            setEdgeOpt(other.getIsEdge().get());
        }
        if (other.getDirection().isPresent()) {
            setDirection(other.getDirection().get());
        }
        if (other.getLabels().isPresent()) {
            setLabels(other.getLabels().get());
        }
        if (other.getPredicate().isPresent()) {
            setPredicate(other.getPredicate().get());
        }
        if (other.getProperties().isPresent()) {
            setProperties(other.getProperties().get());
        }
        if (other.getLimit().isPresent()) {
            setLimit(other.getLimit().get());
        }
    }

    public Optional<OpArg> getLower() {
        return lower;
    }

    public Optional<OpArg> getUpper() {
        return upper;
    }

    public void setLower(OpArg lower) {
        this.lower = Optional.of(lower);
    }

    public void setUpper(OpArg upper) {
        this.upper = Optional.of(upper);
    }
}
