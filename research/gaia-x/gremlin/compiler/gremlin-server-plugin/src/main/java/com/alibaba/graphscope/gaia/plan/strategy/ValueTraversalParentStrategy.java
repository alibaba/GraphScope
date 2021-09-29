package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.gaia.plan.PlanUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.javatuples.Pair;

import java.util.List;
import java.util.Map;

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
                        setParent((ElementValueTraversal) admin, step);
                    }
                });
            } else if (step instanceof GroupCountStep) {
                Traversal.Admin keyTraversal = PlanUtils.getKeyTraversal(step);
                if (keyTraversal != null && keyTraversal instanceof ElementValueTraversal) {
                    setParent((ElementValueTraversal) keyTraversal, step);
                }
            } else if (step instanceof GroupStep) {
                Traversal.Admin keyTraversal = PlanUtils.getKeyTraversal(step);
                if (keyTraversal != null && keyTraversal instanceof ElementValueTraversal) {
                    setParent((ElementValueTraversal) keyTraversal, step);
                }
                Traversal.Admin valueTraversal = PlanUtils.getValueTraversal(step);
                if (valueTraversal != null && valueTraversal instanceof ElementValueTraversal) {
                    setParent((ElementValueTraversal) valueTraversal, step);
                }
            } else if (step instanceof SelectOneStep || step instanceof SelectStep) {
                Map<String, Traversal.Admin> entries = PlanUtils.getSelectTraversalMap(step);
                for (Traversal.Admin admin : entries.values()) {
                    if (admin != null && admin instanceof ElementValueTraversal) {
                        setParent((ElementValueTraversal) admin, step);
                    }
                }
            } else if (step instanceof WherePredicateStep) {
                TraversalRing modulateBy = PlanUtils.getTraversalRing(((WherePredicateStep) step).getLocalChildren(), true);
                for (int k = 0; k < modulateBy.size(); ++k) {
                    Traversal.Admin current = modulateBy.next();
                    if (current != null && current instanceof ElementValueTraversal) {
                        setParent((ElementValueTraversal) current, step);
                    }
                }
            } else if (step instanceof TraversalMapStep) {
                Traversal.Admin admin = (Traversal.Admin) ((TraversalMapStep) step).getLocalChildren().iterator().next();
                if (admin != null && admin instanceof ElementValueTraversal) {
                    setParent((ElementValueTraversal) admin, step);
                }
            }
        }
    }

    protected void setParent(ElementValueTraversal child, Step parent) {
        child.setBypassTraversal(new DefaultTraversal());
        child.setParent((TraversalParent) parent);
    }
}
