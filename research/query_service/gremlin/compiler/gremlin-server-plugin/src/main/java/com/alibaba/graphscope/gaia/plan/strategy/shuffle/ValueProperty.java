/*
 * Copyright 2020 Alibaba Group Holding Limited.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.graphscope.gaia.plan.strategy.shuffle;

import com.alibaba.graphscope.gaia.plan.strategy.PropertyIdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;

import java.util.ArrayList;
import java.util.List;

public class ValueProperty extends PropertyShuffler {
    private List<String> properties = new ArrayList<>();

    /**
     * @param step values("p1") or valueMap("p1")
     */
    public ValueProperty(Step step) {
        super(step);
        if (step instanceof PropertiesStep || step instanceof PropertyMapStep) {
            // todo: set properties
        } else {
            throw new UnsupportedOperationException("cannot support other value step " + step.getClass());
        }
    }

    @Override
    protected boolean match() {
        Traversal.Admin traversal = step.getTraversal();
        // ignore by(values)/by(valueMap)/by(select(keys).values)
        return !(traversal.getSteps().size() == 1 && (traversal.getStartStep() instanceof PropertyMapStep || traversal.getStartStep() instanceof PropertiesStep)
                || traversal.getSteps().size() == 2 && traversal.getStartStep() instanceof TraversalMapStep && traversal.getEndStep() instanceof PropertiesStep);
    }

    @Override
    public int transform() {
        if (!match()) return stepIdx + 1;
        Traversal.Admin traversal = step.getTraversal();
        traversal.addStep(stepIdx, PropertyIdentityStep.createDefault(step));
        return stepIdx + 2;
    }
}
