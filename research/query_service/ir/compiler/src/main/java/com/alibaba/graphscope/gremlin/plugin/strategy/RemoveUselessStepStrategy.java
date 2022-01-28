package com.alibaba.graphscope.gremlin.plugin.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ComputerAwareStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

import java.util.List;

public class RemoveUselessStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final RemoveUselessStepStrategy INSTANCE = new RemoveUselessStepStrategy();

    private RemoveUselessStepStrategy() {
    }

    public static RemoveUselessStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); ++i) {
            Step step = steps.get(i);
            if (step instanceof ComputerAwareStep.EndStep || step instanceof NoOpBarrierStep) {
                traversal.removeStep(step);
            }
        }
    }
}
