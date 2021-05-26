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

import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;
import java.util.Optional;

public class WhereTraversalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final WhereTraversalStrategy INSTANCE = new WhereTraversalStrategy();

    private WhereTraversalStrategy() {
    }

    public static WhereTraversalStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); ++i) {
            Step s = steps.get(i);
            if (s instanceof WhereTraversalStep) {
                Traversal.Admin replaced = new DefaultTraversal();
                List<Traversal.Admin> subTraversal = ((WhereTraversalStep) s).getLocalChildren();
                if (!subTraversal.isEmpty()) {
                    replaced = replaceWhereInnerStep(subTraversal.get(0));
                }
                Step newS = new TraversalFilterStep(traversal, replaced);
                TraversalHelper.copyLabels(s, newS, false);
                traversal.removeStep(s);
                traversal.addStep(i, newS);
            }
        }
    }

    private Traversal.Admin replaceWhereInnerStep(Traversal.Admin traversal) {
        List<Step> steps = traversal.getSteps();
        Traversal.Admin newTraversal = new DefaultTraversal();
        for (int i = 0; i < steps.size(); ++i) {
            newTraversal.addStep(steps.get(i));
        }
        Step step = traversal.getStartStep();
        if (step instanceof WhereTraversalStep.WhereStartStep) {
            WhereTraversalStep.WhereStartStep startStep = (WhereTraversalStep.WhereStartStep) step;
            newTraversal.removeStep(startStep);
            if (!startStep.getScopeKeys().isEmpty()) {
                String tag = (String) startStep.getScopeKeys().iterator().next();
                newTraversal.addStep(0, new SelectOneStep<>(newTraversal, Pop.last, tag));
            }
        }
        step = traversal.getEndStep();
        if (step != EmptyStep.instance() && step instanceof WhereTraversalStep.WhereEndStep) {
            WhereTraversalStep.WhereEndStep lastStep = (WhereTraversalStep.WhereEndStep) step;
            newTraversal.removeStep(lastStep);
            if (!lastStep.getScopeKeys().isEmpty()) {
                String tag = lastStep.getScopeKeys().iterator().next();
                newTraversal.addStep(new WherePredicateStep(newTraversal, Optional.ofNullable(null), P.eq(tag)));
            }
        }
        return newTraversal;
    }
}
