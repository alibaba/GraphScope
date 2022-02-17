/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.gremlin.plugin.step;

import com.alibaba.graphscope.gremlin.exception.ExtendGremlinStepException;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// fuse global ids and labels with the scan operator
public class ScanFusionStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ScanFusionStep.class);
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final List<String> graphLabels = new ArrayList<>();

    public ScanFusionStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, new Object[]{this.returnClass.getSimpleName().toLowerCase(),
                Arrays.toString(this.ids), this.graphLabels, this.hasContainers});
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

    public static boolean processHasLabels(final ScanFusionStep<?, ?> graphStep, final HasContainer hasContainer,
                                           List<HasContainer> originalContainers) {
        if (!hasContainer.getKey().equals(T.label.getAccessor()) || graphStep.getIds().length != 0
                || graphStep.getGraphLabels().size() != 0
                || hasContainer.getBiPredicate() != Compare.eq && hasContainer.getBiPredicate() != Contains.within) {
            return false;
        }
        if (getContainer(originalContainers, T.id.getAccessor()) != null) {
            return false;
        } else {
            P predicate = hasContainer.getPredicate();
            if (predicate.getValue() instanceof List && ((List) predicate.getValue()).size() > 0
                    && ((List) predicate.getValue()).get(0) instanceof String) {
                List<String> values = (List<String>) predicate.getValue();
                values.forEach(k -> graphStep.addGraphLabels(k));
            } else if (predicate.getValue() instanceof String) {
                graphStep.addGraphLabels((String) predicate.getValue());
            } else {
                throw new ExtendGremlinStepException("hasLabel value type not support " + predicate.getValue().getClass());
            }
            return true;
        }
    }

    public static HasContainer getContainer(List<HasContainer> originalContainers, String key) {
        for (HasContainer container : originalContainers) {
            if (container.getKey().equals(key)) return container;
        }
        return null;
    }
}