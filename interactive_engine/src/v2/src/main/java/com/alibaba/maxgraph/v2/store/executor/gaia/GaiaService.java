package com.alibaba.maxgraph.v2.store.executor.gaia;

import com.alibaba.maxgraph.v2.common.MetaService;
import com.alibaba.maxgraph.v2.common.config.CommonConfig;
import com.alibaba.maxgraph.v2.common.config.Configs;
import com.alibaba.maxgraph.v2.common.config.GaiaConfig;
import com.alibaba.maxgraph.v2.store.GraphPartition;
import com.alibaba.maxgraph.v2.store.StoreService;
import com.alibaba.maxgraph.v2.store.executor.ExecutorEngine;

public class GaiaService {

    private Configs configs;
    private ExecutorEngine engine;
    private StoreService storeService;
    private MetaService metaService;

    public GaiaService(Configs configs, ExecutorEngine engine, StoreService storeService, MetaService metaService) {
        this.configs = configs;
        this.engine = engine;
        this.storeService = storeService;
        this.metaService = metaService;
    }

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

    public void stop() {
        this.engine.stop();
    }
}
