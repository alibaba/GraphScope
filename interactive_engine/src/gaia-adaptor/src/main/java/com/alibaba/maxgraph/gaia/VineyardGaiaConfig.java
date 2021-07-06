/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.gaia;

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
        if (strategyFlagName.equals(GaiaConfig.REMOVE_TAG)) {
            return false;
        }
        return true;
    }
}
