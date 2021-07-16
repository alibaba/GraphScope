package com.alibaba.graphscope.gaia.config;

import com.alibaba.graphscope.gaia.store.GraphType;

import java.util.ArrayList;
import java.util.List;

public interface GaiaConfig {
    int DEFAULT_PEGASUS_WORKER_NUM = 1;
    int DEFAULT_PEGASUS_SERVER_NUM = 0;
    int DEFAULT_PEGASUS_TIMEOUT = 60000;
    GraphType DEFAULT_GRAPH_TYPE = GraphType.MAXGRAPH;
    String DEFAULT_SCHEMA_PATH = ".";
    boolean DEFAULT_OPT_FLAG = false;

    String LABEL_PATH_REQUIREMENT = "label_path_requirement";
    String REMOVE_TAG = "remove_tag";
    String PROPERTY_CACHE = "property_cache";

    int getPegasusWorkerNum();

    default List<Long> getPegasusServers() {
        int serverNum = getPegasusServerNum();
        List<Long> servers = new ArrayList<>();
        for (long i = 0; i < serverNum; ++i) {
            servers.add(i);
        }
        return servers;
    }

    default List<String> getPegasusPhysicalHosts() {
        throw new UnsupportedOperationException();
    }

    int getPegasusServerNum();

    long getPegasusTimeout();

    String getSchemaFilePath();

    GraphType getGraphType();

    boolean getOptimizationStrategyFlag(String strategyFlagName);
}
