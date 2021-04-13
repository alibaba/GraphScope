package com.alibaba.graphscope.gaia.plan.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.javatuples.Pair;

import java.util.List;

public class ValueTraversalParentStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> {
    private static final ValueTraversalParentStrategy INSTANCE = new ValueTraversalParentStrategy();

    private ValueTraversalParentStrategy() {
    }

    public static ValueTraversalParentStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<Step> steps = traversal.getSteps();
        for (int i = 0; i < steps.size(); ++i) {
            Step step = steps.get(i);
            if (step instanceof OrderGlobalStep || step instanceof OrderGlobalLimitStep) {
                ((ComparatorHolder) step).getComparators().forEach(k -> {
                    Traversal.Admin admin = (Traversal.Admin) ((Pair) k).getValue0();
                    if (admin instanceof ElementValueTraversal) {
                        ((ElementValueTraversal) admin).setBypassTraversal(new DefaultTraversal());
                        admin.setParent((TraversalParent) step);
                    }
                });
            }
        }
    }
}
