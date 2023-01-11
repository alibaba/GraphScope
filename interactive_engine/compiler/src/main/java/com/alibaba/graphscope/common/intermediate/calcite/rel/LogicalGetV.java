package com.alibaba.graphscope.common.intermediate.calcite.rel;

import com.alibaba.graphscope.common.intermediate.calcite.clause.type.GetVOpt;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class LogicalGetV extends AbstractBindableTableScan {
    private GetVOpt getVOpt;

    public LogicalGetV(
            RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    public LogicalGetV(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, ImmutableList.of(), table);
    }
}
