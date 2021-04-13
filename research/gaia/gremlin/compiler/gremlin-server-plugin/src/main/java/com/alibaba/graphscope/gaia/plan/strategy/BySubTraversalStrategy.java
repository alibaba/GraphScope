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

import com.alibaba.graphscope.gaia.plan.PlanUtils;
import com.alibaba.graphscope.gaia.plan.extractor.TagKeyExtractorFactory;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class BySubTraversalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> {
    private static final BySubTraversalStrategy INSTANCE = new BySubTraversalStrategy();

    private BySubTraversalStrategy() {
    }

    public static BySubTraversalStrategy instance() {
        return INSTANCE;
    }

    /**
     * @param traversal 1. select("a").by(out().count()) -> select("a").subtask({out().count()}, select_joiner) -> do nothing
     *                  2. order().by(out().count()) -> subtask({out().count()}, by_joiner).order().by(sub)
     *                  3. group().by(out().count()).by(out().count()) -> subtask({out().count()}, by_joiner).group().by(sub).subtask(select(values).unfold().out().count())
     */
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        try {
            List<Step> stepList = traversal.getSteps();
            for (int i = 0; i < stepList.size(); ++i) {
                Step step = stepList.get(i);
                if (step instanceof SelectOneStep || step instanceof SelectStep) {
                    Map<String, Traversal.Admin> selectTraversals = PlanUtils.getSelectTraversalMap(step);
                    for (Map.Entry<String, Traversal.Admin> e : selectTraversals.entrySet()) {
                        boolean isSimpleValue = TagKeyExtractorFactory.Select.isSimpleValue(e.getValue());
                        if (step instanceof SelectStep && !isSimpleValue) {
                            // todo: do transform
                            throw new UnsupportedOperationException("cannot support sub traversal in " + step.getClass());
                        } else if (step instanceof SelectOneStep && !isSimpleValue) {
                            FieldUtils.writeField(step, "selectTraversal", null, true);
                            traversal.addStep(++i, new BySubTaskStep(traversal, e.getValue(), BySubTaskStep.JoinerType.Select));
                        }
                    }
                } else if (step instanceof GroupCountStep || step instanceof GroupStep) {
                    Traversal.Admin groupByKey = PlanUtils.getKeyTraversal(step);
                    if (groupByKey != null && !TagKeyExtractorFactory.GroupKeyBy.isSimpleValue(groupByKey)) {
                        traversal.addStep(i++, new BySubTaskStep(traversal, groupByKey, BySubTaskStep.JoinerType.GroupKeyBy));
                        FieldUtils.writeField(step, "keyTraversal", new PreBySubTraversal(), true);
                    }
                    Traversal.Admin groupValue;
                    if (step instanceof GroupStep && (groupValue = PlanUtils.getValueTraversal(step)) != null
                            && !TagKeyExtractorFactory.GroupValueBy.isSimpleValue(groupValue)) {
                        if (step.getNextStep() instanceof UnfoldStep) {
                            ++i;
                        } else {
                            traversal.addStep(++i, new UnfoldStep(traversal));
                        }
                        traversal.addStep(++i, new BySubTaskStep(traversal, addPreStep(groupValue), BySubTaskStep.JoinerType.GroupValueBy));
                        // to_list
                        // todo: distinguish between end fold and count
                        Traversal.Admin newValue = new DefaultTraversal();
                        newValue.addStep(new FoldStep(newValue.asAdmin()));
                        FieldUtils.writeField(step, "valueTraversal", newValue, true);
                    }
                } else if (step instanceof OrderGlobalStep || step instanceof OrderGlobalLimitStep) {
                    List<Pair<Traversal.Admin, Comparator>> toReplace = new ArrayList<>();
                    List<Pair<Traversal.Admin, Comparator>> comparators = ((ComparatorHolder) step).getComparators();
                    for (Pair<Traversal.Admin, Comparator> pair : comparators) {
                        Pair<Traversal.Admin, Comparator> copy = new Pair(pair.getValue0(), pair.getValue1());
                        if (copy.getValue0() != null && !TagKeyExtractorFactory.OrderBY.isSimpleValue(copy.getValue0())) {
                            // fork subtask
                            traversal.addStep(i++, new BySubTaskStep(traversal, copy.getValue0(), BySubTaskStep.JoinerType.OrderBy));
                            copy = copy.setAt0(new PreBySubTraversal());
                        }
                        toReplace.add(copy);
                    }
                    FieldUtils.writeField(step, "comparators", toReplace, true);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("exception is " + e);
        }
    }

    protected Traversal.Admin addPreStep(Traversal.Admin traversal) {
        if (traversal == null) return null;
        traversal.addStep(0, new TraversalMapStep(traversal, new ColumnTraversal(Column.values)));
        traversal.addStep(1, new UnfoldStep(traversal));
        return traversal;
    }
}
