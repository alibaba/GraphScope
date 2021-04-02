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
package com.compiler.demo.server.plan.translator;

import com.compiler.demo.server.plan.PlanUtils;
import com.compiler.demo.server.plan.meta.*;
import com.compiler.demo.server.plan.meta.object.*;
import com.compiler.demo.server.plan.strategy.*;
import com.compiler.demo.server.plan.translator.builder.*;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Function;

public class StepMetaCollector extends AttributeTranslator<StepMetaBuilder, TraverserElement> {
    public StepMetaCollector(StepMetaBuilder input) {
        super(input);
    }

    @Override
    protected Function<StepMetaBuilder, TraverserElement> getApplyFunc() {
        return (StepMetaBuilder t) -> {
            Step step = t.getStep();
            TraversalId metaId = t.getMetaId();
            Step traversalIdStep = step;
            if (metaId.depth() != traversalDepth(step)) {
                // g.V().as("a").order().by(select("a"))
                traversalIdStep = step.getTraversal().getParent().asStep();
            }
            StepId stepId = new StepId(metaId, TraversalHelper.stepIndex(traversalIdStep, traversalIdStep.getTraversal()));
            TraverserElement next = nextTraverser(t, stepId);
            // maintain traversal lifetime
            TraversalsMeta<TraversalId, LifetimeMeta> traversalsLife = (TraversalsMeta) t.getConfig(MetaConfig.TRAVERSALS_LIFETIME);
            TraversalsMeta<TraversalId, PathHistoryMeta> traversalsPath = (TraversalsMeta) t.getConfig(MetaConfig.TRAVERSALS_PATH);
            if (traversalsLife != null) {
                LifetimeMeta lifetime = traversalsLife.get(metaId).get();
                // as("a"): set start_time of next
                if (!lifetime.get(next).isPresent() && !step.getLabels().isEmpty()) {
                    lifetime.add(next, new Lifetime(stepId, null));
                    lifetime.attachLabel(next, (String) step.getLabels().iterator().next());
                }
                if (step instanceof SelectOneStep || step instanceof SelectStep || step instanceof WherePredicateStep) {
                    Set<String> usedTags = new HashSet<>();
                    if (step instanceof SelectStep || step instanceof SelectOneStep) {
                        usedTags.addAll(PlanUtils.getSelectTraversalMap(step).keySet());
                    } else if (step instanceof WherePredicateStep) {
                        usedTags.addAll(PlanUtils.getSelectKeysList(step));
                        if (((WherePredicateStep) step).getStartKey().isPresent()) {
                            usedTags.add((String) ((WherePredicateStep) step).getStartKey().get());
                        }
                    }
                    for (String tag : usedTags) {
                        Step p1 = traversalIdStep;
                        TraversalId p2 = metaId;
                        do {
                            Optional<PathHistoryMeta> currentPathOpt = traversalsPath.get(p2);
                            Optional<LifetimeMeta> currentLifetimeOpt = traversalsLife.get(p2);
                            StepId currentStepId = new StepId(p2, TraversalHelper.stepIndex(p1, p1.getTraversal()));
                            if (currentPathOpt.isPresent()) {
                                PathHistoryMeta currentPath = currentPathOpt.get();
                                for (String k : currentPath.getAllObjects()) {
                                    if (k.equals(tag)) {
                                        // skip repeat(select("a").out()).times(3)
                                        if (currentStepId.equals(stepId) && p1.getTraversal().getParent() instanceof RepeatStep) {
                                            currentLifetimeOpt.get().get(currentPath.get(tag).get()).get().setLastStepId(StepId.KEEP_STEP_ID);
                                        } else {
                                            currentLifetimeOpt.get().get(currentPath.get(tag).get()).get().setLastStepId(currentStepId);
                                        }
                                    }
                                }
                            }
                            p1 = p1.getTraversal().getParent().asStep();
                            p2 = p2.getParent();
                        } while (!(p1 instanceof EmptyStep));
                    }
                }
            }
            // maintain traversal path
            if (traversalsPath != null) {
                Optional<PathHistoryMeta> path = traversalsPath.get(metaId);
                if (path.isPresent()) {
                    for (String label : (Set<String>) step.getLabels()) {
                        path.get().add(label, next);
                    }
                }
            }
            // change traverser type
            TraversalsMeta<TraversalId, TraverserRequirementMeta> traversalsRequire = (TraversalsMeta) t.getConfig(MetaConfig.TRAVERSALS_REQUIREMENT);
            if (traversalsRequire != null) {
                if (step instanceof PathStep || step instanceof PathFilterStep || !step.getLabels().isEmpty()) {
                    Step p1 = traversalIdStep;
                    TraversalId p2 = metaId;
                    do {
                        TraverserRequirementMeta currentRequire = traversalsRequire.get(p2).get();
                        StepId currentStepId = new StepId(p2, TraversalHelper.stepIndex(p1, p1.getTraversal()));
                        if (step instanceof PathStep || step instanceof PathFilterStep || step instanceof PathLocalCountStep) {
                            currentRequire.setPathStepId(currentStepId);
                            // repeat(out().simplePath().out())
                            if (currentStepId.equals(stepId) && p1.getTraversal().getParent() instanceof RepeatStep) {
                                currentRequire.setPathStepId(new StepId(p2, TraversalHelper.stepIndex(p1.getTraversal().getEndStep(), p1.getTraversal())));
                            }
                        } else if (!step.getLabels().isEmpty()) {
                            currentRequire.setLabelPathStepId(currentStepId);
                        }
                        p1 = p1.getTraversal().getParent().asStep();
                        p2 = p2.getParent();
                    } while (!(p1 instanceof EmptyStep));
                }
            }
            return next;
        };
    }

    protected TraverserElement nextTraverser(StepMetaBuilder metaBuilder, StepId stepId) {
        TraverserElement head = metaBuilder.getHead();
        TraversalsMeta<TraversalId, PathHistoryMeta> traversalsPath = (TraversalsMeta) metaBuilder.getConfig(MetaConfig.TRAVERSALS_PATH);
        Step step = metaBuilder.getStep();
        TraversalId metaId = stepId.getTraversalId();
        // V()/E()
        if (step instanceof GraphStep) {
            TraverserElement element;
            if (((GraphStep) step).returnsVertex()) {
                element = new TraverserElement(new CompositeObject(Vertex.class));
            } else {
                element = new TraverserElement(new CompositeObject(Edge.class));
            }
            return element;
        }
        // out/in/both
        if (step instanceof VertexStep) {
            TraverserElement element;
            if (((VertexStep) step).returnsVertex()) {
                element = new TraverserElement(new CompositeObject(Vertex.class));
            } else {
                element = new TraverserElement(new CompositeObject(Edge.class));
            }
            return element;
        }
        if (step instanceof TraversalFilterStep || step instanceof BySubTaskStep) {
            // create sub traversal
            Traversal.Admin sub = ((TraversalParent) step).getLocalChildren().get(0);
            newTraversal(sub, head, metaId.fork(stepId.getStepId(), 0), metaBuilder.getConf()).translate();
            return head;
        }
        if (step instanceof HasStep || step instanceof PropertyIdentityStep || step instanceof IdentityStep || step instanceof PathFilterStep) {
            return head;
        }
        if (step instanceof WherePredicateStep) {
            return head;
        }
        if (step instanceof RepeatStep) {
            // create sub traversal
            Traversal.Admin sub = ((Traversal.Admin) ((RepeatStep) step).getGlobalChildren().get(0));
            TraverserElement loopResult = newTraversal(sub, head, metaId.fork(stepId.getStepId(), 0), metaBuilder.getConf()).translate();
            return new TraverserElement(loopResult.getObject());
        }
        if (step instanceof PathStep) {
            return new TraverserElement(new CompositeObject(List.class, Collections.singletonList(head.getObject())));
        }
        if (step instanceof SelectOneStep) {
            Map.Entry<String, Traversal.Admin> selectOne = PlanUtils.getFirstEntry(PlanUtils.getSelectTraversalMap(step));
            TraverserElement tagHead = (traversalsPath.get(metaId).get()).get(selectOne.getKey()).get();
            Traversal.Admin sub = selectOne.getValue();
            if (sub != null) {
                return newTraversal(sub, tagHead, metaId, metaBuilder.getConf()).translate();
            } else {
                return new TraverserElement(tagHead.getObject());
            }
        }
        if (step instanceof SelectStep) {
            Map<String, Traversal.Admin> selectTraversals = PlanUtils.getSelectTraversalMap(step);
            TraverserElement tagHead = null;
            for (Map.Entry<String, Traversal.Admin> entry : selectTraversals.entrySet()) {
                tagHead = (traversalsPath.get(metaId).get()).get(entry.getKey()).get();
                if (entry.getValue() != null) {
                    tagHead = newTraversal(entry.getValue(), tagHead, metaId, metaBuilder.getConf()).translate();
                }
            }
            return new TraverserElement(new CompositeObject(List.class, Arrays.asList(new CompositeObject(Map.Entry.class,
                    Arrays.asList(new CompositeObject(String.class), tagHead.getObject())))));
        }
        if (step instanceof OrderGlobalStep || step instanceof OrderGlobalLimitStep) {
            ((ComparatorHolder) step).getComparators().forEach(k -> {
                Traversal.Admin sub = (Traversal.Admin) ((Pair) k).getValue0();
                newTraversal(sub, head, metaId, metaBuilder.getConf()).translate();
            });
            return head;
        }
        if (step instanceof GroupCountStep) {
            Traversal.Admin keyTraversal = PlanUtils.getKeyTraversal(step);
            TraverserElement element = head;
            if (keyTraversal != null) {
                element = newTraversal(keyTraversal, head, metaId, metaBuilder.getConf()).translate();
            }
            return new TraverserElement(new CompositeObject(List.class, Arrays.asList(new CompositeObject(Map.Entry.class,
                    Arrays.asList(element.getObject(), new CompositeObject(Integer.class))))));
        }
        if (step instanceof GroupStep) {
            Traversal.Admin keyTraversal = PlanUtils.getKeyTraversal(step);
            TraverserElement value = newTraversal(PlanUtils.getValueTraversal(step), head, metaId, metaBuilder.getConf()).translate();
            TraverserElement key = head;
            if (keyTraversal != null) {
                key = newTraversal(keyTraversal, head, metaId, metaBuilder.getConf()).translate();
            }
            return new TraverserElement(new CompositeObject(List.class, Arrays.asList(new CompositeObject(Map.Entry.class,
                    Arrays.asList(key.getObject(), value.getObject())))));
        }
        if (step instanceof FoldStep) {
            return new TraverserElement(new CompositeObject(List.class, Collections.singletonList(head.getObject())));
        }
        if (step instanceof CountGlobalStep) {
            return new TraverserElement(new CompositeObject(Integer.class));
        }
        if (step instanceof UnionStep) {
            TraverserElement element = null;
            int i = 0;
            for (Object s : ((UnionStep) step).getGlobalChildren()) {
                element = newTraversal((Traversal.Admin) s, head, metaId.fork(stepId.getStepId(), i), metaBuilder.getConf()).translate();
                ++i;
            }
            return new TraverserElement(element.getObject());
        }
        if (step instanceof PropertiesStep) {
            return new TraverserElement(new CompositeObject(String.class));
        }
        if (step instanceof PropertyMapStep) {
            return new TraverserElement(new CompositeObject(List.class, Arrays.asList(new CompositeObject(Map.Entry.class,
                    Arrays.asList(new CompositeObject(String.class), new CompositeObject(String.class))))));
        }
        if (step instanceof TraversalMapStep) {
            Traversal.Admin mapTraversal = (Traversal.Admin) ((TraversalMapStep) step).getLocalChildren().get(0);
            return newTraversal(mapTraversal, head, metaId, metaBuilder.getConf()).translate();
        }
        if (step instanceof PathLocalCountStep) {
            return new TraverserElement(new CompositeObject(Integer.class));
        }
        if (step instanceof EdgeVertexStep || step instanceof EdgeOtherVertexStep) {
            return new TraverserElement(new CompositeObject(Vertex.class));
        }
        if (step instanceof DedupGlobalStep || step instanceof RangeGlobalStep || step instanceof HasAnyStep) {
            return head;
        }
        if (step instanceof UnfoldStep || step instanceof PhysicalPlanUnfoldStep) {
            if (head.getObject().getClassName() != List.class) {
                throw new UnsupportedOperationException("unfold has invalid input type " + head.getObject().getClassName());
            }
            return new TraverserElement(head.getObject().getSubs().get(0));
        }
        throw new UnsupportedOperationException("step not supported " + step.getClass());
    }

    public static TraversalMetaCollector newTraversal(Traversal.Admin sub, TraverserElement head,
                                                      TraversalId subId, Configuration conf) {
        return new TraversalMetaCollector(new TraversalMetaBuilder(sub, head, subId).setConf(conf));
    }

    protected int traversalDepth(Step step) {
        int depth = 0;
        Step p = step;
        while (!(p.getTraversal().getParent() instanceof EmptyStep)) {
            p = p.getTraversal().getParent().asStep();
            ++depth;
        }
        return depth;
    }
}
