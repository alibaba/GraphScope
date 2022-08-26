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

package com.alibaba.graphscope.gremlin.plugin.strategy;

import com.alibaba.graphscope.gremlin.plugin.step.ScanFusionStep;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

public class ScanFusionStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final ScanFusionStepStrategy INSTANCE = new ScanFusionStepStrategy();

    private ScanFusionStepStrategy() {}

    public static ScanFusionStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        for (final GraphStep originalGraphStep :
                TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final ScanFusionStep<?, ?> scanFusionStep = new ScanFusionStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, scanFusionStep, traversal);
            Step<?, ?> currentStep = scanFusionStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    List<HasContainer> originalContainers =
                            ((HasContainerHolder) currentStep).getHasContainers();
                    for (final HasContainer hasContainer : originalContainers) {
                        if (!GraphStep.processHasContainerIds(scanFusionStep, hasContainer)
                                && !ScanFusionStep.processHasLabels(
                                        scanFusionStep, hasContainer, originalContainers)) {
                            scanFusionStep.addHasContainer(hasContainer);
                        }
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
            // fuse scan + coin
            if (currentStep instanceof CoinStep) {
                scanFusionStep.setCoinStep((CoinStep) currentStep);
                TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                traversal.removeStep(currentStep);
            }
        }
    }
}
