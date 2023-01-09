package com.alibaba.graphscope.common.intermediate.core.rel;

import com.alibaba.graphscope.common.intermediate.core.clause.type.ScanOpt;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class LogicalSource extends AbstractBindableTableScan {
    private ScanOpt scanOpt;
    public LogicalSource(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    public LogicalSource(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, ImmutableList.of(), table);
    }
}
