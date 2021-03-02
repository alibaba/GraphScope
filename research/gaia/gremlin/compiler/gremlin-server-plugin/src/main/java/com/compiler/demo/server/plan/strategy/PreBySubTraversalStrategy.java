package com.compiler.demo.server.plan.strategy;

import com.compiler.demo.server.plan.extractor.TagKeyExtractorFactory;
import com.compiler.demo.server.plan.PlanUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class PreBySubTraversalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> {
    private static final PreBySubTraversalStrategy INSTANCE = new PreBySubTraversalStrategy();

    private PreBySubTraversalStrategy() {
    }

    public static PreBySubTraversalStrategy instance() {
        return INSTANCE;
    }

    /**
     * @param traversal 1. select("a").by(out().count()) -> select("a").subtask({out().count()}, select_joiner) -> do nothing
     *                  2. order().by(out().count()) -> subtask({out().count()}, by_joiner).order().by(sub)
     *                  3. group().by(out().count()) -> subtask({out().count()}, by_joiner).group().by(sub)
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
                    if (groupByKey != null && !TagKeyExtractorFactory.GroupBy.isSimpleValue(groupByKey)) {
                        traversal.addStep(i++, new BySubTaskStep(traversal, groupByKey, BySubTaskStep.JoinerType.GroupBy));
                        FieldUtils.writeField(step, "keyTraversal", new PreBySubTraversal(), true);
                    }
                } else if (step instanceof OrderGlobalStep) {
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
}
