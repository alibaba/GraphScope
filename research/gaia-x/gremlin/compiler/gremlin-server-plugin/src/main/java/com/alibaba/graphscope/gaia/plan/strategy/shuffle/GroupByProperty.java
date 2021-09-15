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
import com.alibaba.graphscope.gaia.plan.extractor.TagKeyExtractorFactory;
import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.strategy.OrderGlobalLimitStep;
import com.alibaba.graphscope.gaia.plan.strategy.PropertyIdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.javatuples.Pair;

import java.util.List;

public class GroupByProperty extends PropertyShuffler {
    private Traversal.Admin keyTraversal;

    public GroupByProperty(Step step) {
        super(step);
        if (step instanceof GroupCountStep || step instanceof GroupStep) {
            keyTraversal = PlanUtils.getKeyTraversal(step);
        } else {
            throw new UnsupportedOperationException("cannot support other step in group property " + step.getClass());
        }
    }

    @Override
    protected boolean match() {
        Step previousOut = getPreviousShuffleStep(false);
        // guarantee no select between out and group().by()
        if (previousOut != null) {
            Step p = this.step;
            p = p.getPreviousStep();
            while (p != previousOut) {
                if (p instanceof SelectStep || p instanceof SelectOneStep) return false;
                p = p.getPreviousStep();
            }
        }
        return isGroupByPropertyPattern(this.keyTraversal) || existAfterOrderNestedSelectWithValue(step);
    }

    // group().by("p1")/by(valueMap())
    // todo: group().by().by("p1")
    public static boolean isGroupByPropertyPattern(Traversal.Admin keyTraversal) {
        if (keyTraversal != null && TagKeyExtractorFactory.GroupKeyBy.isSimpleValue(keyTraversal)) {
            Gremlin.TagKey tagKey = TagKeyExtractorFactory.GroupKeyBy.extractFrom(keyTraversal, true);
            if (PlanUtils.isNotSet(tagKey.getTag()) && (tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.KEY
                    && tagKey.getByKey().getKey().getItemCase() == Common.Key.ItemCase.NAME
                    || tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.PROP_KEYS)) {
                return true;
            }
        }
        return false;
    }

    // hack: order().by(select(values).values("id")) after group by
    public static boolean existAfterOrderNestedSelectWithValue(Step step) {
        Traversal.Admin traversal = step.getTraversal();
        if (step == traversal.getEndStep()) {
            return false;
        }
        Step s = step;
        do {
            s = s.getNextStep();
            if (s instanceof OrderGlobalStep || s instanceof OrderGlobalLimitStep) {
                ComparatorHolder holder = (ComparatorHolder) s;
                for (Pair pair : (List<Pair>) holder.getComparators()) {
                    Traversal.Admin sub = (Traversal.Admin) pair.getValue0();
                    if (sub.getSteps().size() == 2 && sub.getStartStep() instanceof TraversalMapStep && sub.getEndStep() instanceof PropertiesStep) {
                        return true;
                    }
                }
            }
        } while (s != traversal.getEndStep());
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
