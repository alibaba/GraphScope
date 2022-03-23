/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.maxgraph.servers.ir;

import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.graphscope.groot.store.GraphPartition;
import com.alibaba.graphscope.groot.store.StoreService;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.servers.AbstractService;

public class GaiaService implements AbstractService {

    private Configs configs;
    private ExecutorEngine engine;
    private StoreService storeService;
    private MetaService metaService;

    public GaiaService(
            Configs configs,
            ExecutorEngine engine,
            StoreService storeService,
            MetaService metaService) {
        this.configs = configs;
        this.engine = engine;
        this.storeService = storeService;
        this.metaService = metaService;
    }

    @Override
    public void start() {
        this.engine.init();
        for (GraphPartition partition : this.storeService.getIdToPartition().values()) {
            this.engine.addPartition(partition);
        }

        int partitionCount = CommonConfig.PARTITION_COUNT.get(this.configs);
        for (int i = 0; i < partitionCount; i++) {
            this.engine.updatePartitionRouting(i, metaService.getStoreIdByPartition(i));
        }
        this.engine.start();
    }

    @Override
    public void stop() {
        this.engine.stop();
    }
}