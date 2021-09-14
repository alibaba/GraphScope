package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.PropertiesCacheStep;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CachePropGaiaGraphStep extends GaiaGraphStep implements PropertiesCacheStep {
    private ToFetchProperties toFetchProperties;

    public CachePropGaiaGraphStep(GaiaGraphStep originalGraphStep) {
        super(originalGraphStep);
        originalGraphStep.getGraphLabels().forEach(k -> this.addGraphLabels((String) k));
        originalGraphStep.getHasContainers().forEach(k -> this.addHasContainer((HasContainer) k));
        this.setTraverserRequirement(originalGraphStep.getTraverserRequirement());
        this.toFetchProperties = new ToFetchProperties(false, Collections.EMPTY_LIST);
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
        keys.addAll(toFetchProperties.getProperties());
        return PlanUtils.convertFrom(new ToFetchProperties(toFetchProperties.isAll(), keys));
    }

    @Override
    public void addPropertiesToCache(ToFetchProperties properties) {
        this.toFetchProperties = properties;
    }
}
