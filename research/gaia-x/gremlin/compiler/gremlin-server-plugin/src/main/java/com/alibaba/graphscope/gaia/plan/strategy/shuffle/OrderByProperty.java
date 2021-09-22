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

import com.alibaba.graphscope.common.proto.Common;
import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.extractor.TagKeyExtractorFactory;
import com.alibaba.graphscope.gaia.plan.strategy.OrderGlobalLimitStep;
import com.alibaba.graphscope.gaia.plan.strategy.PropertyIdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.javatuples.Pair;

import java.util.Comparator;
import java.util.List;

public class OrderByProperty extends PropertyShuffler {
    private ComparatorHolder holder;

    public OrderByProperty(Step step) {
        super(step);
        if (step instanceof OrderGlobalStep || step instanceof OrderGlobalLimitStep) {
            holder = (ComparatorHolder) step;
        } else {
            throw new UnsupportedOperationException("cannot support other step in order by property " + step.getClass());
        }
    }

    // pattern: out().<without select>.order().by(name)
    @Override
    protected boolean match() {
        Step previousOut = getPreviousShuffleStep(false);
        // guarantee no select between out and order().by()
        if (previousOut != null) {
            Step p = this.step;
            p = p.getPreviousStep();
            while (p != previousOut) {
                if (p instanceof SelectStep || p instanceof SelectOneStep) return false;
                p = p.getPreviousStep();
            }
        }
        return isOrderByPropertyPattern(this.holder);
    }

    // order().by(property)
    public static boolean isOrderByPropertyPattern(ComparatorHolder holder) {
        List<org.javatuples.Pair<Traversal.Admin, Comparator>> orderBy = holder.getComparators();
        for (Pair<Traversal.Admin, Comparator> pair : orderBy) {
            Traversal.Admin byTraversal = pair.getValue0();
            if (byTraversal != null && TagKeyExtractorFactory.OrderBY.isSimpleValue(byTraversal)) {
                Gremlin.TagKey orderByKey = TagKeyExtractorFactory.OrderBY.extractFrom(byTraversal);
                // current head with by(property)
                if (PlanUtils.isNotSet(orderByKey.getTag()) && orderByKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.KEY
                        && orderByKey.getByKey().getKey().getItemCase() == Common.Key.ItemCase.NAME) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public int transform() {
        if (!match()) return stepIdx + 1;
        Traversal.Admin traversal = step.getTraversal();
        traversal.addStep(stepIdx, PropertyIdentityStep.createDefault(step));
        return stepIdx + 2;
    }
}
