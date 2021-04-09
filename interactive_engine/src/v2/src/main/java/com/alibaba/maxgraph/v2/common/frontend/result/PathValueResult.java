package com.alibaba.maxgraph.v2.common.frontend.result;

import com.google.common.base.MoreObjects;

import java.util.Set;

/**
 * Value in path result, contains value and label set
 */
public class PathValueResult implements QueryResult {
    private Object value;
    private Set<String> labelList;

    public PathValueResult() {

    }

    public PathValueResult(Object value) {
        this(value, null);
    }

    public PathValueResult(Object value, Set<String> labelList) {
        this.value = value;
        this.labelList = labelList;
    }

    public Object getValue() {
        return value;
    }

    public Set<String> getLabelList() {
        return labelList;
    }

    @Override
    public Object convertToGremlinStructure() {
        return new PathValueResult(
                this.value instanceof QueryResult ? ((QueryResult) this.value).convertToGremlinStructure() : this.value,
                this.labelList);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("value", value)
                .add("labelList", labelList)
                .toString();
    }
}
