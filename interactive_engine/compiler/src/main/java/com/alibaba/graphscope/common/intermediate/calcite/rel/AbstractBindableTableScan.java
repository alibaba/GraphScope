package com.alibaba.graphscope.common.intermediate.calcite.rel;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

import java.util.List;

public abstract class AbstractBindableTableScan extends TableScan {
    // for filter fusion
    private ImmutableList<RexNode> filters;
    // for field trimmer
    private ImmutableIntList project;

    protected AbstractBindableTableScan(
            RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }
}
