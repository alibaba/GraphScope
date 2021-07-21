package com.alibaba.graphscope.gaia;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.store.GraphType;
import com.alibaba.maxgraph.common.cluster.InstanceConfig;

public class MaxGraphConfig implements GaiaConfig {
    private InstanceConfig instanceConfig;

    public MaxGraphConfig(InstanceConfig instanceConfig) {
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
        return GraphType.MAXGRAPH;
    }

    @Override
    public boolean getOptimizationStrategyFlag(String strategyFlagName) {
        if (strategyFlagName.equals(GaiaConfig.REMOVE_TAG)) {
            return false;
        }
        return true;
    }
}
