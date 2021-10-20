/**
 * This file is referred and derived from project apache/tinkerpop
 *
 * <p>https://github.com/apache/tinkerpop/blob/master/neo4j-gremlin/src/main/java/org/apache/tinkerpop/gremlin/neo4j/process/traversal/strategy/optimization/Neo4jGraphStepStrategy.java
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
package com.alibaba.maxgraph.tinkerpop.strategies;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.maxgraph.tinkerpop.steps.MxGraphStep;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public class MxGraphStepStrategy extends AbstractTraversalStrategy<ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static MxGraphStepStrategy INSTANCE = new MxGraphStepStrategy();

    @Override
    public void apply(Admin<?, ?> traversal) {

        if (TraversalHelper.onGraphComputer(traversal)) {
            //    throw TinkerMaxGraph.Exceptions.graphComputerNotSupported();
        }

        for (final GraphStep originalGraphStep :
                TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final MxGraphStep<?, ?> mxGraphStep = new MxGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, mxGraphStep, traversal);
            Step<?, ?> currentStep = mxGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer :
                            ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(mxGraphStep, hasContainer)) {
                            mxGraphStep.addHasContainer(hasContainer);
                        }
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }

            if (currentStep instanceof OrderGlobalStep) {
                List<Pair<Admin, Comparator>> comparators =
                        ((OrderGlobalStep) currentStep).getComparators();
                if (testHasLambdaComparator(comparators) || testHasSubTraversal(comparators)) {
                    return;
                }

                Set<String> keys = new HashSet<>();
                comparators.forEach(
                        c -> {
                            String propertyKey =
                                    ((ElementValueTraversal) c.getLeft()).getPropertyKey();
                            keys.add(propertyKey);
                        });

                Order order = (Order) comparators.get(0).getRight();
                mxGraphStep.setOrder(order, keys);

                Step<?, ?> nextStep = currentStep.getNextStep();
                if (nextStep instanceof RangeGlobalStep) {
                    mxGraphStep.setLimit((int) ((RangeGlobalStep<?>) nextStep).getHighRange());
                }
            }
        }
    }

    private boolean testHasLambdaComparator(final List<Pair<Admin, Comparator>> comparators) {
        return comparators.stream()
                .map(Pair::getRight)
                .anyMatch(c -> c != Order.incr && c != Order.decr);
    }

    private boolean testHasSubTraversal(final List<Pair<Admin, Comparator>> comparators) {
        return comparators.stream()
                .map(Pair::getLeft)
                .anyMatch(t -> !(t instanceof ElementValueTraversal));
    }

    public static MxGraphStepStrategy instance() {
        return INSTANCE;
    }
}
