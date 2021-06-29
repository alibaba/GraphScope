package com.alibaba.maxgraph.v2.frontend.gaia.adaptor;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.store.GraphType;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;

public class VineyardGaiaConfig implements GaiaConfig {
    private InstanceConfig instanceConfig;

    public VineyardGaiaConfig(InstanceConfig instanceConfig) {
        this.instanceConfig = instanceConfig;
    }

    @Override
    public int getPegasusWorkerNum() {
        return instanceConfig.getPegasusWorkerNum();
    }

    @Override
    public int getPegasusServerNum() {
        return instanceConfig.getResourceExecutorCount();
    }

    @Override
    public long getPegasusTimeout() {
        return instanceConfig.getPegasusTimeoutMS();
    }

    @Override
    public String getSchemaFilePath() {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphType getGraphType() {
        return GraphType.VINEYARD;
    }

    @Override
    public boolean getOptimizationStrategyFlag(String strategyFlagName) {
        return true;
    }
}
