package com.alibaba.maxgraph.servers;

import com.alibaba.maxgraph.common.config.CommonConfig;
import com.alibaba.maxgraph.common.config.Configs;
import com.alibaba.maxgraph.servers.ir.IrServiceProducer;

public class ServiceProducerFactory {

    public static ComputeServiceProducer getProducer(Configs configs) {
        String engineType = CommonConfig.ENGINE_TYPE.get(configs).toUpperCase();
        switch (engineType) {
            case "GAIA":
                return new IrServiceProducer(configs);
            default:
                throw new IllegalArgumentException("Unknown engine type [" + engineType + "]");
        }
    }
}
