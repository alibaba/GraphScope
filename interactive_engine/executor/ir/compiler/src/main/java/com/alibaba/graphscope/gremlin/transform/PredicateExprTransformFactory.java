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

package com.alibaba.graphscope.gremlin.transform;

import com.alibaba.graphscope.common.intermediate.ArgUtils;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;

public enum PredicateExprTransformFactory implements PredicateExprTransform {
    HAS_STEP {
        @Override
        public String apply(Step arg) {
            HasContainerHolder hasStep = (HasContainerHolder) arg;
            List<HasContainer> containers = hasStep.getHasContainers();
            String expr = "";
            for (int i = 0; i < containers.size(); ++i) {
                if (i > 0) {
                    expr += " && ";
                }
                HasContainer container = containers.get(i);
                String key = "@." + container.getKey();
                String flatPredicate = flatPredicate(key, container.getPredicate());
                if (i > 0) {
                    expr += "(" + flatPredicate + ")";
                } else {
                    expr += flatPredicate;
                }
            }
            return expr;
        }
    },
    IS_STEP {
        @Override
        public String apply(Step arg) {
            IsStep isStep = (IsStep) arg;
            // current value
            String key = "@";
            return flatPredicate(key, isStep.getPredicate());
        }
    },
    WHERE_END_STEP {
        @Override
        public String apply(Step arg) {
            WhereTraversalStep.WhereEndStep endStep = (WhereTraversalStep.WhereEndStep) arg;
            String matchTag = endStep.getScopeKeys().iterator().next();
            P predicate = P.eq(ArgUtils.asFfiVar(matchTag, ""));
            return flatPredicate("@", predicate);
        }
    }
}
