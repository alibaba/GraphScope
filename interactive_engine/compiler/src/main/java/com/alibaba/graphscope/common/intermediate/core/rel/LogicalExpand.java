package com.alibaba.graphscope.common.intermediate.core.rel;

import com.alibaba.graphscope.common.intermediate.core.clause.type.DirectionOpt;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class LogicalExpand extends AbstractBindableTableScan {
    private DirectionOpt directionOpt;
    public LogicalExpand(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    public LogicalExpand(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, ImmutableList.of(), table);
    }
}
