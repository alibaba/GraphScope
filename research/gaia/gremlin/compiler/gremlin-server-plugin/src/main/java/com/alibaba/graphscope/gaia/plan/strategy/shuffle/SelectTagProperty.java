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
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.javatuples.Pair;

import java.util.*;

public class SelectTagProperty extends PropertyShuffler {
    private List<String> propertyTags = new ArrayList<>();

    public SelectTagProperty(Step step) {
        super(step);
    }

    @Override
    protected boolean match() {
        // todo: accept more patterns: select("a")...order().by("name") / select("a")...has(key, value) / order().by(select("a").by("name"))
        Set<String> tags = step.getLabels();
        for (String tag : tags) {
            if (existSelectProperty(tag)) {
                propertyTags.add(tag);
            }
        }
        return !propertyTags.isEmpty();
    }

    protected boolean existSelectProperty(String tag) {
        Traversal.Admin traversal = step.getTraversal();
        if (step == traversal.getEndStep()) return false;
        Step p = step;
        do {
            p = p.getNextStep();
            if (isSelectByPropertyWithTag(p, tag)) return true;
        } while (p != traversal.getEndStep());
        return false;
    }

    // select(tag).by(property)
    protected boolean isSelectByPropertyWithTag(Step p, String tag) {
        if ((p instanceof SelectStep || p instanceof SelectOneStep) && ((Scoping) p).getScopeKeys().contains(tag)) {
            Traversal.Admin value = PlanUtils.getSelectTraversalMap(p).get(tag);
            if (value != null) {
                // valueMap / by(property) / values("p1")
                // todo: values()
                Gremlin.TagKey keys = TagKeyExtractorFactory.Select.extractFrom(tag, value);
                if (keys.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.PROP_KEYS || keys.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.KEY
                        && keys.getByKey().getKey().getItemCase() == Common.Key.ItemCase.NAME) {
                    return true;
                }
            } else {
                // nearest select(tag).order().by(property)
                // nearest select(tag).group().by(property)
                // todo: nearest select(tag).group().by(...).by(property)
                Step endStep = p.getTraversal().getEndStep();
                if (p == endStep) {
                    return false;
                }
                Step next = p;
                do {
                    next = next.getNextStep();
                    if (next instanceof SelectOneStep || next instanceof SelectStep) {
                        return false;
                    }
                    if ((next instanceof GroupCountStep || next instanceof GroupStep)
                            && GroupByProperty.isGroupByPropertyPattern(PlanUtils.getKeyTraversal(next))
                            || (next instanceof OrderGlobalStep && OrderByProperty.isOrderByPropertyPattern((ComparatorHolder) next))) {
                        return true;
                    }
                } while (next != endStep);
            }
            return false;
        } else if (p instanceof OrderGlobalStep || p instanceof OrderGlobalLimitStep) {
            // order().by(select(tag).by("name")))
            for (Object e : ((ComparatorHolder) p).getComparators()) {
                Traversal.Admin admin = (Traversal.Admin) ((Pair) e).getValue0();
                if (admin != null && admin.getSteps().size() == 1 && admin.getStartStep() instanceof SelectOneStep) {
                    Map.Entry<String, Traversal.Admin> selectOne = PlanUtils.getFirstEntry(
                            PlanUtils.getSelectTraversalMap(admin.getStartStep()));
                    Gremlin.TagKey tagKey = TagKeyExtractorFactory.Select.extractFrom(selectOne.getKey(), selectOne.getValue());
                    if (tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.KEY
                            && tagKey.getByKey().getKey().getItemCase() == Common.Key.ItemCase.NAME) {
                        return true;
                    }
                }
            }
            return false;
        } else if (p instanceof GroupCountStep || p instanceof GroupStep) {
            // group()/groupCount().by(select(tag).by("name"))/by(select(tag).by(valueMap(...)))
            Traversal.Admin keyTraversal = PlanUtils.getKeyTraversal(p);
            if (keyTraversal != null && keyTraversal.getSteps().size() == 1 && keyTraversal.getStartStep() instanceof SelectOneStep) {
                Map.Entry<String, Traversal.Admin> selectOne = PlanUtils.getFirstEntry(
                        PlanUtils.getSelectTraversalMap(keyTraversal.getStartStep()));
                Gremlin.TagKey tagKey = TagKeyExtractorFactory.Select.extractFrom(selectOne.getKey(), selectOne.getValue());
                if (tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.KEY
                        && tagKey.getByKey().getKey().getItemCase() == Common.Key.ItemCase.NAME
                        || tagKey.getByKey().getItemCase() == Gremlin.ByKey.ItemCase.PROP_KEYS) {
                    return true;
                }
            }
            return false;
        } else {
            // todo: add more pattern
            return false;
        }
    }

    @Override
    public int transform() {
        if (!match()) return stepIdx + 1;
        PropertyIdentityStep newStep = PropertyIdentityStep.createDefault(step);
        for (String tag : propertyTags) {
            this.step.removeLabel(tag);
            newStep.addLabel(tag);
        }
        Traversal.Admin traversal = step.getTraversal();
        traversal.addStep(stepIdx + 1, newStep);
        return stepIdx + 2;
    }
}
