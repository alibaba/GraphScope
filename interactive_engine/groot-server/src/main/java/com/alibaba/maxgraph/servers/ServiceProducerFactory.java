package com.alibaba.maxgraph.servers;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.servers.maxgraph.MaxGraphServiceProducer;

public class ServiceProducerFactory {

    public static ComputeServiceProducer getProducer(Configs configs) {
        String engineType = CommonConfig.ENGINE_TYPE.get(configs).toUpperCase();
        switch (engineType) {
            case "MAXGRAPH":
                return new MaxGraphServiceProducer(configs);
            default:
                throw new IllegalArgumentException("Unknown engine type [" + engineType + "]");
        }
    }
}
