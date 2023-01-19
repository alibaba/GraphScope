package com.alibaba.graphscope.common.calcite.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class LogicalPathExpand extends AbstractRelNode {
    // LogicalExpand with LogicalGetV
    private RelNode expandGetV;

    protected LogicalPathExpand(@Nullable RelOptCluster cluster, RelNode expandGetV) {
        super(cluster, RelTraitSet.createEmpty());
        this.expandGetV = expandGetV;
    }
}
