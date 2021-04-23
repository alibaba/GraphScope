package com.alibaba.maxgraph.v2.frontend.compiler.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Map;
import java.util.NoSuchElementException;

public class MaxGraphStep<S, E extends Element> extends GraphStep<S, E> {
    private Map<String, Object> queryConfig;

    public MaxGraphStep(Map<String, Object> queryConfig, final Traversal.Admin traversal, final Class<E> returnClass, final boolean isStart, final Object... ids) {
        super(traversal, returnClass, isStart, ids);
        this.queryConfig = queryConfig;
    }

    public Map<String, Object> getQueryConfig() {
        return queryConfig;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        return super.processNextStart();
    }

    @Override
    public void onGraphComputer() {
        super.onGraphComputer();
    }
}
