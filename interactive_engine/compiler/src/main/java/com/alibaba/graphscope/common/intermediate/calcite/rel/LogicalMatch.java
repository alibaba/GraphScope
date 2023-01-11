package com.alibaba.graphscope.common.intermediate.calcite.rel;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.NotImplementedException;

public class LogicalMatch {
    private ImmutableList<RelNode> sentences;

    public RelNode toJoin() {
        throw new NotImplementedException("");
    }
}
