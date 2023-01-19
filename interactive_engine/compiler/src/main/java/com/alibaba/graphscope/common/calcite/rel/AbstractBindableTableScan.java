package com.alibaba.graphscope.common.calcite.rel;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * a basic structure of graph operators
 */
public abstract class AbstractBindableTableScan extends TableScan {
    // for filter fusion
    protected ImmutableList<RexNode> filters;
    // for field trimmer
    protected ImmutableIntList project;

    protected List<RelOptTable> tables;

    protected AbstractBindableTableScan(
            @Nullable RelOptCluster cluster,
            @Nullable List<RelHint> hints,
            List<RelOptTable> tables) {
        super(cluster, RelTraitSet.createEmpty(), hints, tables.isEmpty() ? null : tables.get(0));
    }

    @Override
    public RelDataType deriveRowType() {
        return null;
    }
}
