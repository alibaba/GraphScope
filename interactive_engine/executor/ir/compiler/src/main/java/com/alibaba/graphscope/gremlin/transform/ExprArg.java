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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.PropertyType;

import java.util.ArrayList;
import java.util.List;

public class ExprArg {
    // store all steps of the traversal given by arg
    private List<Step> stepsInTraversal = new ArrayList<>();

    // judge whether the traversal is expression or apply, i.e
    // IdentityTraversal -> "@" is a expression pattern,
    // ValueTraversal("name") -> "@.name" is a expression pattern,
    // other common traversal type containing list of steps need judge further by
    // TraversalParentTransform
    public ExprArg(Traversal.Admin traversal) {
        if (traversal == null || traversal instanceof IdentityTraversal) {
            // do nothing
        } else if (traversal instanceof ValueTraversal) {
            stepsInTraversal.add(
                    new PropertiesStep(
                            traversal,
                            PropertyType.VALUE,
                            ((ValueTraversal) traversal).getPropertyKey()));
        } else {
            traversal.getSteps().forEach(k -> stepsInTraversal.add((Step) k));
        }
    }

    // provide a more flexible constructor to judge whether list of steps is expression or apply,
    // i.e
    // group().by().by(values("name").count) need aggregate by [values("name").count()],
    // [values] is extracted from the list and passed as the argument of the function
    public ExprArg(List<Step> steps) {
        stepsInTraversal = steps;
    }

    public boolean isEmpty() {
        return stepsInTraversal.isEmpty();
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
}
