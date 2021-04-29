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
package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.gaia.store.StaticGraphStore;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SchemaIdMakerStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final Logger logger = LoggerFactory.getLogger(SchemaIdMakerStrategy.class);
    private static final SchemaIdMakerStrategy INSTANCE = new SchemaIdMakerStrategy();

    private SchemaIdMakerStrategy() {
    }

    public static SchemaIdMakerStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = traversal.getSteps();
        // point to the same object after RepeatUnroll, avoid this
        Set<Step> converted = new HashSet<>();
        for (int i = 0; i < steps.size(); ++i) {
            Step step = steps.get(i);
            if (converted.contains(step)) {
                continue;
            }
            if (step instanceof HasContainerHolder) {
                List<HasContainer> containers = ((HasContainerHolder) step).getHasContainers();
                for (HasContainer container : containers) {
                    if (container.getKey().equals(T.label.getAccessor())) {
                        P<String> predicate = (P<String>) container.getPredicate();
                        long labelId = StaticGraphStore.INSTANCE.getLabelId(predicate.getValue());
                        if (labelId == StaticGraphStore.INVALID_ID) {
                            logger.error("label id is invalid, check label {} exists", predicate.getValue());
                            return;
                        }
                        predicate.setValue(String.valueOf(labelId));
                        converted.add(step);
                    }
                }
            } else if (step instanceof VertexStep) {
                String[] edgeLabels = ((VertexStep) step).getEdgeLabels();
                for (int j = 0; j < edgeLabels.length; ++j) {
                    long labelId = StaticGraphStore.INSTANCE.getLabelId(edgeLabels[j]);
                    if (labelId == StaticGraphStore.INVALID_ID) {
                        logger.error("label id is invalid, check label {} exists", edgeLabels[j]);
                        return;
                    }
                    edgeLabels[j] = String.valueOf(labelId);
                    converted.add(step);
                }
            }
        }
    }
    // todo: property id
}
