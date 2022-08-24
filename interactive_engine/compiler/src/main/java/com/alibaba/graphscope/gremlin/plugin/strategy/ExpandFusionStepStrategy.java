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

import com.alibaba.graphscope.common.jna.type.FfiExpandOpt;
import com.alibaba.graphscope.gremlin.plugin.step.ExpandFusionStep;
import com.alibaba.graphscope.gremlin.plugin.step.GroupStep;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExpandFusionStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final Set<Class<? extends OptimizationStrategy>> PRIORS =
            new HashSet(Arrays.asList(InlineFilterStrategy.class));
    private static final ExpandFusionStepStrategy INSTANCE = new ExpandFusionStepStrategy();

    public static ExpandFusionStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        for (final VertexStep originalVertexStep :
                TraversalHelper.getStepsOfClass(VertexStep.class, traversal)) {
            final ExpandFusionStep<?> expandFusionStep = new ExpandFusionStep<>(originalVertexStep);
            TraversalHelper.replaceStep(originalVertexStep, expandFusionStep, traversal);
            if (expandFusionStep.getExpandOpt() == FfiExpandOpt.Edge) {
                // fuse outE + filter
                Step<?, ?> currentStep = expandFusionStep.getNextStep();
                while (currentStep instanceof HasStep) {
                    List<HasContainer> originalContainers =
                            ((HasContainerHolder) currentStep).getHasContainers();
                    for (final HasContainer hasContainer : originalContainers) {
                        expandFusionStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                    currentStep = currentStep.getNextStep();
                }
                // fuse outE + inV
                currentStep = expandFusionStep.getNextStep();
                if (expandFusionStep.getLabels().isEmpty()
                        && currentStep instanceof EdgeVertexStep
                        && canFuseExpandWithGetV(expandFusionStep, (EdgeVertexStep) currentStep)) {
                    expandFusionStep.setExpandOpt(FfiExpandOpt.Vertex);
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
            }
        }
        // fuse out + count nested in apply
        if (isApply(traversal)
                && traversal.getSteps().size() == 2
                && traversal.getStartStep() instanceof ExpandFusionStep
                && traversal.getEndStep() instanceof CountGlobalStep) {
            ExpandFusionStep expandStep = (ExpandFusionStep) traversal.getStartStep();
            CountGlobalStep countStep = (CountGlobalStep) traversal.getEndStep();
            expandStep.setExpandOpt(FfiExpandOpt.Degree);
            TraversalHelper.copyLabels(countStep, countStep.getPreviousStep(), false);
            traversal.removeStep(countStep);
        }
    }

    private boolean isApply(Traversal.Admin admin) {
        Step parent = admin.getParent().asStep();
        return parent instanceof SelectOneStep
                || parent instanceof SelectStep
                || parent instanceof DedupGlobalStep
                || parent instanceof OrderGlobalStep
                || parent instanceof GroupStep
                || parent instanceof GroupCountStep
                || parent instanceof WhereTraversalStep
                || parent instanceof TraversalFilterStep
                || parent instanceof WherePredicateStep
                || parent instanceof NotStep;
    }

    private boolean canFuseExpandWithGetV(
            ExpandFusionStep expandFusionStep, EdgeVertexStep edgeVertexStep) {
        return expandFusionStep.getDirection() == Direction.OUT
                        && edgeVertexStep.getDirection() == Direction.IN
                || expandFusionStep.getDirection() == Direction.IN
                        && edgeVertexStep.getDirection() == Direction.OUT
                || expandFusionStep.getDirection() == Direction.BOTH
                        && edgeVertexStep.getDirection() == Direction.BOTH;
    }
}
