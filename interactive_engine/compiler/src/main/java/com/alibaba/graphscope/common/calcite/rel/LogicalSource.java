package com.alibaba.graphscope.common.calcite.rel;

import com.alibaba.graphscope.common.calcite.rel.builder.config.ScanOpt;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;

public class LogicalSource extends AbstractBindableTableScan {
    private ScanOpt scanOpt;

    protected LogicalSource(RelOptCluster cluster, List<RelHint> hints, List<RelOptTable> tables) {
        super(cluster, hints, tables);
    }

    public static LogicalSource create(
            RelOptCluster cluster, List<RelHint> hints, List<RelOptTable> tables) {
        return new LogicalSource(cluster, hints, tables);
    }
}
