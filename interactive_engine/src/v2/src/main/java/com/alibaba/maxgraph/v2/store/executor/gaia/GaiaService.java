package com.alibaba.maxgraph.v2.store.executor.gaia;

import com.alibaba.maxgraph.v2.store.GraphPartition;
import com.alibaba.maxgraph.v2.store.StoreService;
import com.alibaba.maxgraph.v2.store.executor.ExecutorEngine;

public class GaiaService {

    private ExecutorEngine engine;
    private StoreService storeService;

    public GaiaService(ExecutorEngine engine, StoreService storeService) {
        this.engine = engine;
        this.storeService = storeService;
    }

    public void start() {
        this.engine.init();
        for (GraphPartition partition : this.storeService.getIdToPartition().values()) {
            this.engine.addPartition(partition);
        }
        this.engine.start();
    }

    public void stop() {
        this.engine.stop();
    }
}
