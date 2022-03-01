package com.alibaba.graphscope.common.store;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.GraphConfig;
import com.alibaba.graphscope.gremlin.Utils;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ExperimentalStoreConfigs implements StoreConfigs {
    private Logger logger = LoggerFactory.getLogger(ExperimentalStoreConfigs.class);
    private Configs configs;

    public ExperimentalStoreConfigs(Configs configs) {
        this.configs = configs;
    }

    @Override
    public Map<String, Object> getConfigs() {
        String schemaFilePath = GraphConfig.GRAPH_SCHEMA.get(configs);
        try {
            String schema = Utils.readStringFromFile(schemaFilePath);
            return ImmutableMap.of("graph.schema", schema);
        } catch (IOException e) {
            logger.info("open schema file {} fail", schemaFilePath);
            throw new RuntimeException(e);
        }
    }
}
