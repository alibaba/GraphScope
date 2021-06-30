package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.PropertiesCache;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.List;

public class CachePropGraphStep extends GaiaGraphStep implements PropertiesCache {
    public CachePropGraphStep(GraphStep originalGraphStep) {
        super(originalGraphStep);
    }

    @Override
    public Gremlin.PropKeys cacheProperties() {
        List<String> keys = new ArrayList<>();
        List<HasContainer> containers = this.getHasContainers();
        for (HasContainer container : containers) {
            if (!container.getKey().equals(T.label.getAccessor()) && !container.getKey().equals(T.id.getAccessor())) {
                keys.add(container.getKey());
            }
        }
        return PlanUtils.convertFrom(new ToFetchProperties(false, keys));
    }

    @Override
    public void addPropertiesToCache(ToFetchProperties properties) {
        throw new UnsupportedOperationException();
    }
}
