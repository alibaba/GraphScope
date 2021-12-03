package com.alibaba.graphscope.gaia.plan.strategy.global.property.cache;

import com.alibaba.graphscope.common.proto.Gremlin;

public interface PropertiesCacheStep {
    Gremlin.PropKeys cacheProperties();

    void addPropertiesToCache(ToFetchProperties properties);
}
