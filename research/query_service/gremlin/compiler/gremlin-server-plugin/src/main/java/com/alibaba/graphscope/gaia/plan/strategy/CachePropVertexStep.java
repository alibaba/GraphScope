package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.PropertiesCacheStep;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.Iterator;

public class CachePropVertexStep<E extends Element> extends VertexStep<E> implements PropertiesCacheStep {
    private ToFetchProperties toFetchProperties;

    public CachePropVertexStep(final VertexStep orig) {
        super(orig.getTraversal(), orig.getReturnClass(), orig.getDirection(), orig.getEdgeLabels());
        this.toFetchProperties = new ToFetchProperties(false, Collections.EMPTY_LIST);
    }

    @Override
    public Gremlin.PropKeys cacheProperties() {
        return PlanUtils.convertFrom(this.toFetchProperties);
    }

    @Override
    public void addPropertiesToCache(ToFetchProperties properties) {
        this.toFetchProperties = properties;
    }

    @Override
    protected Iterator flatMap(Traverser.Admin traverser) {
        return null;
    }
}
