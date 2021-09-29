package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.gaia.config.GaiaConfig;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

public class CachePropStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> {
    private GaiaConfig gaiaConfig;
    private static final CachePropStepStrategy INSTANCE = new CachePropStepStrategy();

    private CachePropStepStrategy() {
    }

    public static CachePropStepStrategy instance(GaiaConfig config) {
        if (INSTANCE.gaiaConfig == null) {
            INSTANCE.gaiaConfig = config;
        }
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<Step> stepList = traversal.getSteps();
        for (int i = 0; i < stepList.size(); ++i) {
            Step step = stepList.get(i);
            if (step instanceof GaiaGraphStep || step instanceof VertexStep) {
                Step newStep;
                if (step instanceof GaiaGraphStep) {
                    newStep = new CachePropGaiaGraphStep((GaiaGraphStep) step);
                } else {
                    newStep = new CachePropVertexStep((VertexStep) step);
                }
                TraversalHelper.copyLabels(step, newStep, false);
                traversal.removeStep(step);
                traversal.addStep(i, newStep);
            }
        }
    }
}
