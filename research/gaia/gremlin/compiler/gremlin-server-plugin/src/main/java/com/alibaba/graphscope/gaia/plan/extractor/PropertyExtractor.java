package com.alibaba.graphscope.gaia.plan.extractor;

import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import org.apache.tinkerpop.gremlin.process.traversal.Step;

public interface PropertyExtractor {
    ToFetchProperties extractProperties(Step step);
}
