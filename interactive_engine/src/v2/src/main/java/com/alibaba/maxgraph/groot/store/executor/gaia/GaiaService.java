package com.alibaba.maxgraph.groot.store.executor.gaia;

import com.alibaba.maxgraph.groot.common.MetaService;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.groot.store.GraphPartition;
import com.alibaba.maxgraph.groot.store.StoreService;
import com.alibaba.maxgraph.groot.store.executor.ExecutorEngine;

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
