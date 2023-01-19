package com.alibaba.graphscope.common.calcite.rel;

import com.alibaba.graphscope.common.calcite.rel.builder.config.DirectionOpt;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class LogicalExpand extends AbstractBindableTableScan {
    private DirectionOpt directionOpt;

    protected LogicalExpand(RelOptCluster cluster, List<RelHint> hints, List<RelOptTable> tables) {
        super(cluster, hints, tables);
    }

    public static LogicalExpand create(
            RelOptCluster cluster, List<RelHint> hints, List<RelOptTable> tables) {
        return new LogicalExpand(cluster, hints, tables);
    }
}
