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

import com.alibaba.graphscope.gaia.plan.strategy.shuffle.HasStepProperty;
import com.alibaba.graphscope.gaia.plan.strategy.shuffle.IdentityProperty;
import com.alibaba.graphscope.gaia.plan.strategy.shuffle.PropertyShuffler;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

public class OrderGuaranteeStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> {
    private static final OrderGuaranteeStrategy INSTANCE = new OrderGuaranteeStrategy();

    private OrderGuaranteeStrategy() {
    }

    public static OrderGuaranteeStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = traversal.getSteps();
        int lastOrderBy = getLastOrderBy(traversal);
        if (lastOrderBy == -1) return;
        // guarantee order after last OrderBy
        for (int i = lastOrderBy + 1; i < steps.size(); ++i) {
            Step step = steps.get(i);
            // add shuffle implicitly
            if (step instanceof HasStep && (new HasStepProperty((HasStep) step)).needShuffle()
                    || step instanceof PropertyIdentityStep && (new IdentityProperty((PropertyIdentityStep) step)).needShuffle()) {
                Step p = step.getPreviousStep();
                boolean existShuffleStep = false;
                while (!(p instanceof EmptyStep) && !(p instanceof OrderGlobalStep || p instanceof OrderGlobalLimitStep)) {
                    if (p instanceof VertexStep || PropertyShuffler.isGlobalStep(step)) {
                        existShuffleStep = true;
                        break;
                    }
                    p = p.getPreviousStep();
                }
                if (!existShuffleStep && (p instanceof OrderGlobalStep || p instanceof OrderGlobalLimitStep)) {
                    int orderIdx = TraversalHelper.stepIndex(p, traversal);
                    traversal.removeStep(step);
                    traversal.addStep(orderIdx, step);
                }
            }
        }
    }

    protected int getLastOrderBy(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = traversal.getSteps();
        int index = -1;
        for (int i = steps.size() - 1; i >= 0; --i) {
            Step step = steps.get(i);
            if (step instanceof OrderGlobalLimitStep || step instanceof OrderGlobalStep) {
                return i;
            }
        }
        return index;
    }
}
