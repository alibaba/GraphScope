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
package com.alibaba.graphscope.gaia.plan.strategy.shuffle;

import com.alibaba.graphscope.gaia.plan.strategy.PropertyIdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.List;

public class HasStepProperty extends PropertyShuffler {
    public HasStepProperty(HasStep step) {
        super(step);
    }

    @Override
    public int transform() {
        if (!match()) return stepIdx + 1;
        Traversal.Admin traversal = step.getTraversal();
        List<HasContainer> propertyList = new ArrayList<>(), schemaKeyList = new ArrayList<>();
        extractHasProperty(((HasStep) step).getHasContainers(), propertyList, schemaKeyList);
        traversal.removeStep(step);
        traversal.addStep(stepIdx, PropertyIdentityStep.createDefault(step));
        if (schemaKeyList.isEmpty()) {
            HasStep propertyStep = new HasStep(traversal, asArray(propertyList));
            TraversalHelper.copyLabels(step, propertyStep, false);
            traversal.addStep(stepIdx + 1, propertyStep);
            return stepIdx + 2;
        } else {
            traversal.addStep(stepIdx + 1, new HasStep(traversal, asArray(schemaKeyList)));
            HasStep propertyStep = new HasStep(traversal, asArray(propertyList));
            TraversalHelper.copyLabels(step, propertyStep, false);
            traversal.addStep(stepIdx + 2, propertyStep);
            return stepIdx + 3;
        }
    }

    @Override
    protected boolean match() {
        List<HasContainer> containerList = ((HasStep) step).getHasContainers();
        for (HasContainer container : containerList) {
            if (!container.getKey().equals(T.id.getAccessor()) && !container.getKey().equals(T.label.getAccessor())) {
                return true;
            }
        }
        return false;
    }

    protected void extractHasProperty(List<HasContainer> source, List<HasContainer> propertyList, List<HasContainer> schemaKeyList) {
        source.forEach(h -> {
            if (h.getKey().equals(T.label.getAccessor()) || h.getKey().equals(T.id.getAccessor())) {
                schemaKeyList.add(h);
            } else {
                propertyList.add(h);
            }
        });
    }

    protected HasContainer[] asArray(List<HasContainer> list) {
        return list.toArray(new HasContainer[list.size()]);
    }
}
