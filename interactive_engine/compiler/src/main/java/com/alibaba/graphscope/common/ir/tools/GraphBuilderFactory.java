package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.config.Configs;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.tools.RelBuilderFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GraphBuilderFactory implements RelBuilderFactory {
    private final Configs configs;

    public GraphBuilderFactory(Configs configs) {
        this.configs = configs;
    }

    @Override
    public GraphBuilder create(RelOptCluster relOptCluster, @Nullable RelOptSchema relOptSchema) {
        return GraphBuilder.create(configs, (GraphOptCluster) relOptCluster, relOptSchema);
    }
}
