/**
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
package com.compiler.demo.server.plan;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.pegasus.builder.JobBuilder;
import com.alibaba.pegasus.service.proto.PegasusClient;
import com.compiler.demo.server.plan.extractor.TagKeyExtractorFactory;
import com.compiler.demo.server.plan.resource.GremlinStepResource;
import com.compiler.demo.server.plan.resource.JobBuilderResource;
import com.compiler.demo.server.plan.resource.StepResource;
import com.compiler.demo.server.plan.predicate.HasContainerP;
import com.compiler.demo.server.plan.predicate.WherePredicateP;
import com.compiler.demo.server.plan.strategy.BySubTaskStep;
import com.compiler.demo.server.plan.strategy.MaxGraphStep;
import com.compiler.demo.server.plan.strategy.OrderGlobalLimitStep;
import com.compiler.demo.server.plan.strategy.PropertyIdentityStep;
import com.compiler.demo.server.plan.translator.PredicateTranslator;
import com.compiler.demo.server.plan.translator.TraversalTranslator;
import com.google.protobuf.ByteString;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;

// todo: avoid import *
import java.util.*;
import java.util.stream.Collectors;

public class LogicPlanGlobalMap {
    public enum STEP {
        GraphStep,
        MaxGraphStep,
        TraversalFilterStep,
        HasStep,
        WherePredicateStep,
        VertexStep,
        RepeatStep,
        PathStep,
        RangeGlobalStep,
        PathFilterStep,
        SelectOneStep,
        SelectStep,
        PropertyIdentityStep,
        OrderGlobalStep,
        OrderGlobalLimitStep,
        GroupStep,
        GroupCountStep,
        CountGlobalStep,
        BySubTaskStep,
        UnionStep,
        PropertiesStep,
        TraversalMapStep,
        PathLocalCountStep,
        IdentityStep,
        EdgeVertexStep,
        DedupGlobalStep
    }

    public static STEP stepType(Step t) {
        return STEP.valueOf(t.getClass().getSimpleName());
    }

    private final static Map<STEP, StepResource> stepPlanMap;

    static {
        stepPlanMap = new HashMap();
        stepPlanMap.put(STEP.GraphStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                return Gremlin.GraphStep.newBuilder().addAllIds(PlanUtils.intIdsAsLongList(((GraphStep) t).getIds()))
                        .setReturnType(((GraphStep) t).returnsVertex() ? Gremlin.EntityType.VERTEX : Gremlin.EntityType.EDGE)
                        .build();
            }
        });
        stepPlanMap.put(STEP.MaxGraphStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                Gremlin.GraphStep.Builder builder = Gremlin.GraphStep.newBuilder()
                        .addAllIds(PlanUtils.intIdsAsLongList(((MaxGraphStep) t).getIds()))
                        .setReturnType(((GraphStep) t).returnsVertex() ? Gremlin.EntityType.VERTEX : Gremlin.EntityType.EDGE)
                        .setPredicates(new PredicateTranslator(new HasContainerP((MaxGraphStep) t)).translate());
                List<String> edgeLabels = ((MaxGraphStep) t).getGraphLabels();
                if (!edgeLabels.isEmpty()) {
                    edgeLabels.forEach(l -> builder.addLabels(Integer.valueOf(l)));
                }
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.HasStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                return Gremlin.HasStep.newBuilder()
                        .setPredicates(new PredicateTranslator(new HasContainerP((HasStep) t)).translate())
                        .build();
            }
        });
        stepPlanMap.put(STEP.TraversalFilterStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step t, JobBuilder builder) {
                Traversal.Admin traversal = (Traversal.Admin) ((TraversalFilterStep) t).getLocalChildren().get(0);
                builder.forkJoin(Gremlin.SubTaskJoiner.newBuilder()
                        .setWhereJoiner(Gremlin.WhereJoiner.newBuilder()).build()
                        .toByteString(), new TraversalTranslator(traversal).translate());
            }
        });
        stepPlanMap.put(STEP.WherePredicateStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                WherePredicateStep t1 = (WherePredicateStep) t;
                TraversalRing modulateBy = PlanUtils.getTraversalRing(t1.getLocalChildren(), true);
                Gremlin.WhereStep.Builder builder = Gremlin.WhereStep.newBuilder()
                        .setStartToken(TagKeyExtractorFactory.WherePredicate.extractFrom(modulateBy.next()).getByKey().getKey());
                // add predicates
                Optional<P<?>> predicateOpt = t1.getPredicate();
                if (predicateOpt.isPresent()) {
                    builder.setPredicates(new PredicateTranslator(new WherePredicateP(predicateOpt.get(), modulateBy)).translate());
                }
                // add start tag
                Optional<String> startOpt = t1.getStartKey();
                if (startOpt.isPresent()) {
                    builder.setStartTag(startOpt.get());
                }
                // add other tags
                List<String> tags = PlanUtils.getSelectKeysList(t1);
                if (tags != null && !tags.isEmpty()) {
                    builder.addAllTags(tags);
                }
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.VertexStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                Gremlin.VertexStep.Builder builder = Gremlin.VertexStep.newBuilder()
                        .setReturnType(((VertexStep) t).returnsVertex() ? Gremlin.EntityType.VERTEX : Gremlin.EntityType.EDGE)
                        .setDirection(Gremlin.Direction.valueOf(((VertexStep) t).getDirection().name()));
                List<String> edgeLabels = Arrays.asList(((VertexStep) t).getEdgeLabels());
                if (!edgeLabels.isEmpty()) {
                    edgeLabels.forEach(l -> builder.addEdgeLabels(Integer.valueOf(l)));
                }
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.RepeatStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                // add repeat_traversal
                List<Step> steps = ((Traversal.Admin) ((RepeatStep) step).getGlobalChildren().get(0)).getSteps();
                Traversal.Admin repeat = new DefaultTraversal();
                for (Step s : steps) {
                    repeat.addStep(s);
                }
                Step endStep = repeat.getEndStep();
                if (endStep != EmptyStep.instance() && endStep instanceof RepeatStep.RepeatEndStep) {
                    repeat.removeStep(endStep);
                }
                // add loops
                Traversal.Admin traversal = ((RepeatStep) step).getUntilTraversal();
                long times = 0;
                if (traversal instanceof LoopTraversal) {
                    times = ((LoopTraversal) traversal).getMaxLoops();
                }
                target.repeat((int) times, new TraversalTranslator(repeat).translate());
            }
        });
        stepPlanMap.put(STEP.PathStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                return Gremlin.PathStep.newBuilder()
                        .build();
            }
        });
        stepPlanMap.put(STEP.PathFilterStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                return Gremlin.PathFilterStep.newBuilder()
                        .setHint(((PathFilterStep) t).isSimple() ?
                                Gremlin.PathFilterStep.PathHint.SIMPLE : Gremlin.PathFilterStep.PathHint.CYCLIC)
                        .build();
            }
        });
        stepPlanMap.put(STEP.RangeGlobalStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                target.limit(true, (int) ((RangeGlobalStep) step).getHighRange());
            }
        });
        stepPlanMap.put(STEP.SelectOneStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                Map.Entry<String, Traversal.Admin> selectOne = PlanUtils.getFirstEntry(PlanUtils.getSelectTraversalMap(step));
                Gremlin.GremlinStep.Builder stepBuilder = Gremlin.GremlinStep.newBuilder();
                // todo: hack way to implement select("a") temporarily
                if (selectOne.getValue() == null) {
                    stepBuilder.setSelectOneWithoutBy(Gremlin.SelectOneStepWithoutBy.newBuilder().setTag(selectOne.getKey()));
                } else {
                    stepBuilder.setSelectStep(Gremlin.SelectStep.newBuilder()
                            .setPop(PlanUtils.convertFrom(((SelectOneStep) step).getPop()))
                            .addSelectKeys(TagKeyExtractorFactory.Select.extractFrom(selectOne.getKey(), selectOne.getValue())));
                }
                target.map(stepBuilder.build().toByteString());
            }
        });
        stepPlanMap.put(STEP.SelectStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                Map<String, Traversal.Admin> selectTraversals = PlanUtils.getSelectTraversalMap(step);
                Gremlin.SelectStep.Builder builder = Gremlin.SelectStep.newBuilder().setPop(PlanUtils.convertFrom(((SelectStep) step).getPop()));
                selectTraversals.forEach((k, v) -> {
                    builder.addSelectKeys(TagKeyExtractorFactory.Select.extractFrom(k, v));
                });
                target.map(Gremlin.GremlinStep.newBuilder().setSelectStep(builder).build().toByteString());
            }
        });
        stepPlanMap.put(STEP.PropertyIdentityStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                return Gremlin.IdentityStep.newBuilder().setIsAll(((PropertyIdentityStep) t).isNeedAll())
                        .addAllProperties(((PropertyIdentityStep) t).getAttachProperties())
                        .build();
            }
        });
        stepPlanMap.put(STEP.OrderGlobalStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                target.sortBy(true, Gremlin.GremlinStep.newBuilder()
                        .setOrderByStep(PlanUtils.constructFrom((ComparatorHolder) step))
                        .build().toByteString());
            }
        });
        stepPlanMap.put(STEP.OrderGlobalLimitStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                target.topBy(true, ((OrderGlobalLimitStep) step).getLimit(), Gremlin.GremlinStep.newBuilder()
                        .setOrderByStep(PlanUtils.constructFrom((ComparatorHolder) step))
                        .build().toByteString());
            }
        });
        stepPlanMap.put(STEP.GroupCountStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                Gremlin.GremlinStep.Builder builder = Gremlin.GremlinStep.newBuilder().setGroupByStep(PlanUtils.constructFrom(step));
                target.groupBy(true, builder.build().toByteString(), PegasusClient.AccumKind.CNT, ByteString.EMPTY);
            }
        });
        stepPlanMap.put(STEP.GroupStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                Gremlin.GremlinStep.Builder builder = Gremlin.GremlinStep.newBuilder().setGroupByStep(PlanUtils.constructFrom(step));
                target.groupBy(true, builder.build().toByteString(), PlanUtils.getAccumKind(step), ByteString.EMPTY);
            }
        });
        stepPlanMap.put(STEP.CountGlobalStep, new JobBuilderResource() {
            @Override
            public void buildJob(Step step, JobBuilder target) {
                target.count(true);
            }
        });
        stepPlanMap.put(STEP.BySubTaskStep, new JobBuilderResource() {
            @Override
            protected void buildJob(Step t, JobBuilder target) {
                Traversal.Admin subTraversal = ((BySubTaskStep) t).getBySubTraversal();
                BySubTaskStep.JoinerType type = ((BySubTaskStep) t).getJoiner();
                if (subTraversal != null) {
                    target.forkJoin(PlanUtils.getByJoiner(type).toByteString(), new TraversalTranslator(subTraversal).translate());
                }
            }
        });
        stepPlanMap.put(STEP.UnionStep, new JobBuilderResource() {
            @Override
            protected void buildJob(Step t, JobBuilder target) {
                target.union((List<JobBuilder>) ((UnionStep) t).getGlobalChildren().stream()
                        .map(k -> new TraversalTranslator((Traversal.Admin) k).translate())
                        .collect(Collectors.toList()));
            }
        });
        stepPlanMap.put(STEP.PathLocalCountStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                return Gremlin.PathLocalCountStep.newBuilder().build();
            }
        });
        stepPlanMap.put(STEP.IdentityStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                return Gremlin.IdentityStep.newBuilder().setIsAll(false).build();
            }
        });
        stepPlanMap.put(STEP.TraversalMapStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                Traversal.Admin mapTraversal = (Traversal.Admin) ((TraversalMapStep) t).getLocalChildren().iterator().next();
                return Gremlin.SelectStep.newBuilder()
                        .addSelectKeys(TagKeyExtractorFactory.TraversalMap.extractFrom(mapTraversal))
                        .build();
            }
        });
        stepPlanMap.put(STEP.PropertiesStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                Gremlin.ValuesStep.Builder builder = Gremlin.ValuesStep.newBuilder();
                String[] properties = ((PropertiesStep) t).getPropertyKeys();
                if (properties != null && properties.length > 0) {
                    builder.addAllProperties(Arrays.asList(properties));
                }
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.EdgeVertexStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t) {
                return Gremlin.EdgeVertexStep.newBuilder()
                        .setDirection(Gremlin.Direction.valueOf(((EdgeVertexStep) t).getDirection().name()))
                        .build();
            }
        });
        stepPlanMap.put(STEP.DedupGlobalStep, new JobBuilderResource() {
            @Override
            protected void buildJob(Step t, JobBuilder target) {
                target.dedup(true);
            }
        });
    }

    public static Optional<StepResource> getResourceConstructor(STEP step) {
        return Optional.ofNullable(stepPlanMap.get(step));
    }
}
