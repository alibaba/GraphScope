package com.alibaba.graphscope.common.calcite.rel;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.NotImplementedException;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * a wrapper structure for all related graph operators
 */
public class LogicalMatch extends AbstractRelNode {
    private ImmutableList<RelNode> sentences;

    protected LogicalMatch(@Nullable RelOptCluster cluster, @Nullable RelTraitSet traitSet) {
        super(cluster, traitSet);
    }

    public RelNode toJoin() {
        throw new NotImplementedException("");
    }
}
