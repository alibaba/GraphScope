package com.alibaba.graphscope.common.calcite.rel;

import com.alibaba.graphscope.common.calcite.rel.builder.config.GetVOpt;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class LogicalGetV extends AbstractBindableTableScan {
    private GetVOpt getVOpt;

    protected LogicalGetV(RelOptCluster cluster, List<RelHint> hints, List<RelOptTable> tables) {
        super(cluster, hints, tables);
    }

    public static LogicalGetV create(
            RelOptCluster cluster, List<RelHint> hints, List<RelOptTable> tables) {
        return new LogicalGetV(cluster, hints, tables);
    }
}
