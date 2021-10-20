/**
 * This file is referred and derived from project apache/tinkerpop
 *
 * <p>https://github.com/apache/tinkerpop/blob/master/tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/process/traversal/step/sideEffect/TinkerGraphStep.java
 *
 * <p>which has the following license:
 *
 * <p>Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.maxgraph.tinkerpop.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.alibaba.maxgraph.structure.graph.TinkerMaxGraph;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class MxGraphStep<S, E extends Element> extends GraphStep<S, E>
        implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();
    private Pair<Boolean, Set<String>> order;
    private Integer limit;

    public MxGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(
                originalGraphStep.getTraversal(),
                originalGraphStep.getReturnClass(),
                originalGraphStep.isStartStep(),
                originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);

        // we used to only setIteratorSupplier() if there were no ids OR the first id was instanceof
        // Element,
        // but that allowed the filter in g.V(v).has('k','v') to be ignored.  this created problems
        // for
        // PartitionStrategy which wants to prevent someone from passing "v" from one
        // TraversalSource to
        // another TraversalSource using a different partition
        this.setIteratorSupplier(
                () ->
                        (Iterator<E>)
                                (Vertex.class.isAssignableFrom(this.returnClass)
                                        ? this.vertices()
                                        : this.edges()));
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(hasContainers);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else {
            this.hasContainers.add(hasContainer);
        }
    }

    public void setOrder(@Nonnull Order order, @Nonnull final Set<String> key) {
        Set<String> keys = Collections.unmodifiableSet(key);
        this.order = Pair.of(order == Order.incr, keys);
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    private Iterator<? extends Vertex> vertices() {
        TinkerMaxGraph graph = (TinkerMaxGraph) this.getTraversal().getGraph().get();
        return graph.vertices(ids);
    }

    private Iterator<? extends Edge> edges() {
        throw new UnsupportedOperationException("g.E().has(...)...");
    }
}
