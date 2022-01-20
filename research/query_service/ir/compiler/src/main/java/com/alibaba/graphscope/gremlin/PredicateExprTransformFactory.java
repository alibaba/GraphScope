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
package com.alibaba.graphscope.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public enum PredicateExprTransformFactory implements PredicateExprTransform {
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
    },
    EXPR_FROM_WHERE_PREDICATE {
        @Override
        public String apply(Object arg) {
            WherePredicateStep step = (WherePredicateStep) arg;
            Optional<String> startKey = step.getStartKey();
            TraversalRing traversalRing = Utils.getFieldValue(WherePredicateStep.class, step, "traversalRing");

            String startTag = startKey.isPresent() ? startKey.get() : "";
            String startBy = ByTraversalTransformFactory.getTagByTraversalAsExpr(startTag, traversalRing.next());

            P predicate = (P) step.getPredicate().get();
            List<String> selectKeys = Utils.getFieldValue(WherePredicateStep.class, step, "selectKeys");
            traverseAndUpdateP(predicate, selectKeys.iterator(), traversalRing);

            return flatPredicate(startBy, predicate);
        }

        private void traverseAndUpdateP(P predicate, Iterator<String> selectKeysIterator, TraversalRing traversalRing) {
            if (predicate instanceof ConnectiveP) {
                ((ConnectiveP) predicate).getPredicates().forEach(p1 -> {
                    traverseAndUpdateP((P) p1, selectKeysIterator, traversalRing);
                });
            } else {
                String tagProperty = ByTraversalTransformFactory.getTagByTraversalAsExpr(selectKeysIterator.next(), traversalRing.next());
                predicate.setValue(new WherePredicateValue(tagProperty));
            }
        }
    };

    public class WherePredicateValue {
        private String predicateValue;

        public WherePredicateValue(String predicateValue) {
            this.predicateValue = predicateValue;
        }

        @Override
        public String toString() {
            return predicateValue;
        }
    }
}
