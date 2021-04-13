/*
 * This file is referred and derived from project apache/tinkerpop
 *
 *   https://github.com/apache/tinkerpop/blob/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/process/traversal/step/map/GraphStep.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.gaia.store.StaticGraphStore;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class MaxGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MaxGraphStep.class);
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final List<String> graphLabels = new ArrayList<>();
    // default
    private TraverserRequirement traverserRequirement = TraverserRequirement.PATH;

    public MaxGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return (null == this.ids || 0 == this.ids.length) ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    public TraverserRequirement getTraverserRequirement() {
        return traverserRequirement;
    }

    public void setTraverserRequirement(TraverserRequirement traverserRequirement) {
        this.traverserRequirement = traverserRequirement;
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }


    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    public List<String> getGraphLabels() {
        return Collections.unmodifiableList(this.graphLabels);
    }

    public void addGraphLabels(String label) {
        this.graphLabels.add(label);
    }

    /**
     * label + id -> global_id
     */
    public static boolean processPrimaryKey(final MaxGraphStep<?, ?> graphStep, final HasContainer hasContainer,
                                            final List<HasContainer> originalContainers, boolean[] primaryKeyAsIndex) {
        if (graphStep.ids.length == 0 && isValidPrimaryKey(hasContainer)
                && (hasContainer.getKey().equals(T.label.getAccessor()) && isValidPrimaryKey(getContainer(originalContainers, "id"))
                || hasContainer.getKey().equals("id") && isValidPrimaryKey(getContainer(originalContainers, T.label.getAccessor())))) {
            primaryKeyAsIndex[0] = true;
        }

        if (!hasContainer.getKey().equals(T.label.getAccessor()) && !hasContainer.getKey().equals("id")
                || hasContainer.getBiPredicate() != Compare.eq && hasContainer.getBiPredicate() != Contains.within
                || !isFirstHasContainerWithKey(hasContainer, originalContainers) || !primaryKeyAsIndex[0]) {
            return false;
        }

        if (primaryKeyAsIndex[0] && hasContainer.getKey().equals(T.label.getAccessor())) {
            HasContainer propertyIdContainer = getContainer(originalContainers, "id");
            long globalId = StaticGraphStore.INSTANCE.getGlobalId(
                    Long.valueOf((String) hasContainer.getPredicate().getValue()),
                    ((Number) propertyIdContainer.getPredicate().getValue()).longValue()
            );
            if (globalId == StaticGraphStore.INVALID_ID) {
                logger.error("global id is invalid, check label {} and propertyId {}",
                        hasContainer.getPredicate().getValue(), propertyIdContainer.getPredicate().getValue());
            } else {
                graphStep.addIds(globalId);
            }
        }
        return true;
    }

    public static boolean processHasLabels(final MaxGraphStep<?, ?> graphStep, final HasContainer hasContainer,
                                           List<HasContainer> originalContainers) {
        if (!hasContainer.getKey().equals(T.label.getAccessor()) || graphStep.getIds().length != 0
                || graphStep.getGraphLabels().size() != 0
                || hasContainer.getBiPredicate() != Compare.eq && hasContainer.getBiPredicate() != Contains.within) {
            return false;
        }
        if (getContainer(originalContainers, T.id.getAccessor()) != null) {
            return false;
        } else {
            graphStep.addGraphLabels((String) hasContainer.getPredicate().getValue());
            return true;
        }
    }

    public static boolean isFirstHasContainerWithKey(final HasContainer hasContainer, final List<HasContainer> originalContainers) {
        String key = hasContainer.getKey();
        boolean result = true;
        for (HasContainer container : originalContainers) {
            if (container == hasContainer) break;
            if (container.getKey().equals(key)) return false;
        }
        return result;
    }

    public static HasContainer getContainer(List<HasContainer> originalContainers, String key) {
        for (HasContainer container : originalContainers) {
            if (container.getKey().equals(key)) return container;
        }
        return null;
    }

    public static boolean isValidPrimaryKey(HasContainer container) {
        return container != null &&
                (container.getKey().equals(T.label.getAccessor()) || container.getKey().equals("id")) &&
                (container.getBiPredicate() == Compare.eq || container.getBiPredicate() == Contains.within);
    }
}
