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
package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import com.alibaba.graphscope.gaia.store.GraphType;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;

public class MaxGraphConfig implements GaiaConfig {
    private Configs configs;

    public MaxGraphConfig(Configs configs) {
        this.configs = configs;
    }

    @Override
    public int getPegasusWorkerNum() {
        return PegasusConfig.PEGASUS_WORKER_NUM.get(configs);
    }

    @Override
    public int getPegasusServerNum() {
        return CommonConfig.STORE_NODE_COUNT.get(configs);
    }

    @Override
    public long getPegasusTimeout() {
        return PegasusConfig.PEGASUS_TIMEOUT.get(configs);
    }

    @Override
    public int getPegasusBatchSize() {
        return PegasusConfig.PEGASUS_BATCH_SIZE.get(configs);
    }

    @Override
    public int getPegasusOutputCapacity() {
        return PegasusConfig.PEGASUS_OUTPUT_CAPACITY.get(configs);
    }

    @Override
    public int getPegasusMemoryLimit() {
        return PegasusConfig.PEGASUS_MEMORY_LIMIT.get(configs);
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
