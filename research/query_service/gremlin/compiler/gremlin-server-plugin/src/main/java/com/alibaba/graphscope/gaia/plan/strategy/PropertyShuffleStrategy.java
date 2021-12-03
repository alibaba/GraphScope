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
package com.alibaba.graphscope.gaia.plan.strategy;

import com.alibaba.graphscope.gaia.plan.strategy.shuffle.*;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.List;

public class PropertyShuffleStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> {
    private static final PropertyShuffleStrategy INSTANCE = new PropertyShuffleStrategy();

    private PropertyShuffleStrategy() {
    }

    public static PropertyShuffleStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        List<Step> stepList = traversal.getSteps();
        for (int i = 0; i < stepList.size(); ) {
            Step step = stepList.get(i);
            PropertyShuffler shuffler;
            if (step instanceof HasStep) {
                shuffler = new HasStepProperty((HasStep) step);
                i = shuffler.transform();
                step = stepList.get(i - 1);
            } else if (step instanceof OrderGlobalStep || step instanceof OrderGlobalLimitStep) {
                shuffler = new OrderByProperty(step);
                i = shuffler.transform();
                step = stepList.get(i - 1);
            } else if (step instanceof GroupStep || step instanceof GroupCountStep) {
                shuffler = new GroupByProperty(step);
                i = shuffler.transform();
                step = stepList.get(i - 1);
            } else if (step instanceof PropertiesStep || step instanceof PropertyMapStep) {
                shuffler = new ValueProperty(step);
                i = shuffler.transform();
                step = stepList.get(i - 1);
            }
            // has("name","xx").as("a")
            shuffler = new SelectTagProperty(step);
            i = shuffler.transform();
        }
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
