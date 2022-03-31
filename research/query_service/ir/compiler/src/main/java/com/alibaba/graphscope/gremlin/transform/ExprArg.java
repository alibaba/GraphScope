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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ExprArg {
    // store all steps of the traversal given by arg
    private List<Step> stepsInTraversal;
    // judge whether the traversal is instance of IdentityTraversal, i.e. by()
    private boolean isIdentityTraversal;
    // judge whether the traversal is instance of ValueTraversal, i.e. by('name'),
    // a property name is present if it is
    private Optional<String> propertyKeyOpt;

    public ExprArg() {
        isIdentityTraversal = false;
        propertyKeyOpt = Optional.empty();
        stepsInTraversal = new ArrayList<>();
    }

    public ExprArg(Traversal.Admin traversal) {
        this();
        if (traversal == null || traversal instanceof IdentityTraversal) {
            isIdentityTraversal = true;
        } else if (traversal instanceof ValueTraversal) {
            propertyKeyOpt = Optional.of(((ValueTraversal) traversal).getPropertyKey());
        } else {
            traversal
                    .asAdmin()
                    .getSteps()
                    .forEach(
                            k -> {
                                stepsInTraversal.add((Step) k);
                            });
        }
    }

    public boolean isIdentityTraversal() {
        return isIdentityTraversal;
    }

    public int size() {
        return stepsInTraversal.size();
    }

    public Step getStartStep() {
        return stepsInTraversal.isEmpty() ? EmptyStep.instance() : stepsInTraversal.get(0);
    }

    public Step getEndStep() {
        return stepsInTraversal.isEmpty() ? EmptyStep.instance() : stepsInTraversal.get(size() - 1);
    }

    public Optional<String> getPropertyKeyOpt() {
        return propertyKeyOpt;
    }

    public ExprArg addStep(Step step) {
        this.stepsInTraversal.add(step);
        return this;
    }
}
