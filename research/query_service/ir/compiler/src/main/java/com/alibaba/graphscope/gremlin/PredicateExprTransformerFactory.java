package com.alibaba.graphscope.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;

public enum PredicateExprTransformerFactory implements PredicateExprTransformer {
    EXPR_FROM_CONTAINERS {
        @Override
        public String apply(Object arg) {
            List<HasContainer> containers = (List<HasContainer>) arg;
            String expr = "";
            for (int i = 0; i < containers.size(); ++i) {
                if (i > 0) {
                    expr += " && ";
                }
                HasContainer container = containers.get(i);
                String key = "@." + container.getKey();
                expr += flatPredicate(key, container.getPredicate());
            }
            return expr;
        }
    },
    EXPR_FROM_IS_STEP {
        @Override
        public String apply(Object arg) {
            IsStep isStep = (IsStep) arg;
            // current value
            String key = "@";
            return flatPredicate(key, isStep.getPredicate());
        }
    }
}