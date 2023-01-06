package com.alibaba.graphscope.groot.servers;

import com.alibaba.graphscope.groot.common.config.CommonConfig;
import com.alibaba.graphscope.groot.common.config.Configs;
import com.alibaba.graphscope.groot.servers.ir.IrServiceProducer;

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
