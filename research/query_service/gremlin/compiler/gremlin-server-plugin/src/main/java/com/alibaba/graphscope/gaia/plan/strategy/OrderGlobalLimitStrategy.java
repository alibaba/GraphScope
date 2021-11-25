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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

public class OrderGlobalLimitStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> {
    private static final OrderGlobalLimitStrategy INSTANCE = new OrderGlobalLimitStrategy();

    private OrderGlobalLimitStrategy() {
    }

    public static OrderGlobalLimitStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); ++i) {
            Step step = steps.get(i);
            if (step instanceof OrderGlobalStep && step.getNextStep() instanceof RangeGlobalStep) {
                RangeGlobalStep nextStep = (RangeGlobalStep) step.getNextStep();
                OrderGlobalLimitStep newStep = new OrderGlobalLimitStep((OrderGlobalStep) step, (int) nextStep.getHighRange());
                TraversalHelper.copyLabels(step, newStep, false);
                TraversalHelper.copyLabels(nextStep, newStep, false);
                traversal.removeStep(step);
                traversal.removeStep(nextStep);
                traversal.addStep(i, newStep);
            }
        }
    }
}
