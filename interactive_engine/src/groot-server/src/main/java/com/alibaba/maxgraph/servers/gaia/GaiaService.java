package com.alibaba.maxgraph.servers.gaia;

import com.alibaba.graphscope.groot.meta.MetaService;
import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.graphscope.groot.store.GraphPartition;
import com.alibaba.graphscope.groot.store.StoreService;
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
