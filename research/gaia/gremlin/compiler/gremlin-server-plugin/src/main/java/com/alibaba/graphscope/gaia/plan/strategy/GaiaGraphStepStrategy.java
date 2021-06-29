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

import com.alibaba.graphscope.gaia.store.GraphStoreService;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

public class GaiaGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final GaiaGraphStepStrategy INSTANCE = new GaiaGraphStepStrategy();
    private GraphStoreService graphStore;

    private GaiaGraphStepStrategy() {
    }

    public static GaiaGraphStepStrategy instance(GraphStoreService graphStore) {
        if (INSTANCE.graphStore == null) {
            INSTANCE.graphStore = graphStore;
        }
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final GaiaGraphStep<?, ?> maxGraphStep = new GaiaGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, maxGraphStep, traversal);
            Step<?, ?> currentStep = maxGraphStep.getNextStep();
            boolean[] primaryKeyAsIndex = new boolean[]{false};
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    List<HasContainer> originalContainers = ((HasContainerHolder) currentStep).getHasContainers();
                    for (final HasContainer hasContainer : originalContainers) {
                        if (!GraphStep.processHasContainerIds(maxGraphStep, hasContainer) &&
                                !GaiaGraphStep.processPrimaryKey(maxGraphStep, hasContainer, originalContainers, primaryKeyAsIndex, graphStore) &&
                                !GaiaGraphStep.processHasLabels(maxGraphStep, hasContainer, originalContainers)) {
                            maxGraphStep.addHasContainer(hasContainer);
                        }
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }
}
