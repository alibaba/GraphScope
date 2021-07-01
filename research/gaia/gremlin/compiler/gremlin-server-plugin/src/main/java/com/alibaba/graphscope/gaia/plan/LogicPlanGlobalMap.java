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
package com.alibaba.graphscope.gaia.plan;

import com.alibaba.graphscope.common.proto.Gremlin;
import com.alibaba.graphscope.gaia.idmaker.IdMaker;
import com.alibaba.graphscope.gaia.plan.extractor.PropertyExtractor;
import com.alibaba.graphscope.gaia.plan.meta.object.*;
import com.alibaba.graphscope.gaia.plan.strategy.*;
import com.alibaba.graphscope.gaia.plan.strategy.global.TransformTraverserStep;
import com.alibaba.graphscope.gaia.FilterHelper;
import com.alibaba.graphscope.gaia.plan.translator.builder.PlanConfig;
import com.alibaba.graphscope.gaia.store.SnapshotIdFetcher;
import com.alibaba.graphscope.gaia.plan.strategy.global.property.cache.ToFetchProperties;
import com.alibaba.pegasus.builder.JobBuilder;
import com.alibaba.pegasus.builder.ReduceBuilder;
import com.alibaba.graphscope.gaia.plan.extractor.TagKeyExtractorFactory;
import com.alibaba.graphscope.gaia.plan.resource.GremlinStepResource;
import com.alibaba.graphscope.gaia.plan.resource.JobBuilderResource;
import com.alibaba.graphscope.gaia.plan.resource.StepResource;
import com.alibaba.graphscope.gaia.plan.predicate.HasContainerP;
import com.alibaba.graphscope.gaia.plan.predicate.WherePredicateP;
import com.alibaba.graphscope.gaia.plan.translator.PredicateTranslator;
import com.alibaba.graphscope.gaia.plan.translator.TraversalTranslator;
import com.alibaba.graphscope.gaia.plan.translator.builder.StepBuilder;
import com.alibaba.graphscope.gaia.plan.translator.builder.TraversalBuilder;
import com.google.protobuf.ByteString;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.LoopTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.structure.Direction;

// todo: avoid import *
import java.util.*;
import java.util.stream.Collectors;

public class LogicPlanGlobalMap {
    public enum STEP {
        GraphStep,
        GaiaGraphStep,
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
        PropertyMapStep,
        TraversalMapStep,
        PathLocalCountStep,
        IdentityStep,
        EdgeVertexStep,
        DedupGlobalStep,
        UnfoldStep,
        EdgeOtherVertexStep,
        PhysicalPlanUnfoldStep,
        TransformTraverserStep,
        HasAnyStep,
        IsStep,
        FoldStep,
        CachePropGaiaGraphStep,
        CachePropVertexStep
    }

    public static STEP stepType(Step t) {
        return STEP.valueOf(t.getClass().getSimpleName());
    }

    private final static Map<STEP, StepResource> stepPlanMap;

    static {
        stepPlanMap = new HashMap();
        stepPlanMap.put(STEP.GraphStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                return Gremlin.GraphStep.newBuilder().addAllIds(PlanUtils.extractIds(((GraphStep) t).getIds()))
                    .setReturnType(((GraphStep) t).returnsVertex() ? Gremlin.EntityType.VERTEX : Gremlin.EntityType.EDGE)
                    .addTraverserRequirements(Gremlin.TraverserRequirement.PATH)
                    .build();
            }
        });
        stepPlanMap.put(STEP.GaiaGraphStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                Gremlin.GraphStep.Builder builder = Gremlin.GraphStep.newBuilder()
                    .addAllIds(PlanUtils.extractIds(((GaiaGraphStep) t).getIds()))
                    .setReturnType(((GraphStep) t).returnsVertex() ? Gremlin.EntityType.VERTEX : Gremlin.EntityType.EDGE)
                    .setPredicates(new PredicateTranslator(new HasContainerP((GaiaGraphStep) t)).translate())
                    .addTraverserRequirements(Gremlin.TraverserRequirement.valueOf(((GaiaGraphStep) t).getTraverserRequirement().name()));
                List<String> edgeLabels = ((GaiaGraphStep) t).getGraphLabels();
                if (!edgeLabels.isEmpty()) {
                    edgeLabels.forEach(l -> builder.addLabels(Integer.valueOf(l)));
                }
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.CachePropGaiaGraphStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                Gremlin.GraphStep.Builder builder = Gremlin.GraphStep.newBuilder()
                    .addAllIds(PlanUtils.extractIds(((CachePropGaiaGraphStep) t).getIds()))
                    .setReturnType(((GraphStep) t).returnsVertex() ? Gremlin.EntityType.VERTEX : Gremlin.EntityType.EDGE)
                    .setPredicates(new PredicateTranslator(new HasContainerP((CachePropGaiaGraphStep) t)).translate())
                    .addTraverserRequirements(Gremlin.TraverserRequirement.valueOf(((CachePropGaiaGraphStep) t).getTraverserRequirement().name()))
                    .setRequiredProperties(((CachePropGaiaGraphStep) t).cacheProperties());
                List<String> edgeLabels = ((CachePropGaiaGraphStep) t).getGraphLabels();
                if (!edgeLabels.isEmpty()) {
                    edgeLabels.forEach(l -> builder.addLabels(Integer.valueOf(l)));
                }
                SnapshotIdFetcher fetcher = (SnapshotIdFetcher) conf.getProperty(PlanConfig.SNAPSHOT_ID_FETCHER);
                if (fetcher != null) {
                    builder.setSnapshotId(Gremlin.SnapShotId.newBuilder().setId(fetcher.getSnapshotId()));
                }
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.HasStep, new JobBuilderResource() {
                @Override
                protected void buildJob(StepBuilder stepBuilder) {
                    Step t = stepBuilder.getStep();
                    JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                    target.filter(Gremlin.GremlinStep.newBuilder().setHasStep(Gremlin.HasStep.newBuilder()
                        .setPredicates(new PredicateTranslator(new HasContainerP((HasStep) t)).translate())).build().toByteString());
                }
            }
        );
        stepPlanMap.put(STEP.TraversalFilterStep, new JobBuilderResource() {
            @Override
            public void buildJob(StepBuilder stepBuilder) {
                Step t = stepBuilder.getStep();
                Configuration conf = stepBuilder.getConf();
                JobBuilder builder = (JobBuilder) stepBuilder.getJobBuilder();
                Traversal.Admin traversal = (Traversal.Admin) ((TraversalFilterStep) t).getLocalChildren().get(0);
                builder.forkJoin(Gremlin.SubTaskJoiner.newBuilder()
                    .setWhereJoiner(Gremlin.WhereJoiner.newBuilder()).build()
                    .toByteString(), (JobBuilder) new TraversalTranslator((new TraversalBuilder(traversal)).setConf(conf)).translate());
            }
        });
        stepPlanMap.put(STEP.WherePredicateStep, new JobBuilderResource() {
            @Override
            protected void buildJob(StepBuilder stepBuilder) {
                Step t = stepBuilder.getStep();
                Configuration conf = stepBuilder.getConf();
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
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
                IdMaker tagIdMaker = PlanUtils.getTagIdMaker(conf);
                if (startOpt.isPresent()) {
                    builder.setStartTag(Gremlin.StepTag.newBuilder().setTag((int) tagIdMaker.getId(startOpt.get())));
                }
                // add other tags
                List<String> tags = PlanUtils.getSelectKeysList(t1);
                if (tags != null && !tags.isEmpty()) {
                    tags.forEach(tag -> builder.addTags(Gremlin.StepTag.newBuilder().setTag((int) tagIdMaker.getId(tag))));
                }
                target.filter(Gremlin.GremlinStep.newBuilder().setWhereStep(builder).build().toByteString());
            }
        });
        stepPlanMap.put(STEP.VertexStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
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
        stepPlanMap.put(STEP.CachePropVertexStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                Gremlin.VertexStep.Builder builder = Gremlin.VertexStep.newBuilder()
                    .setReturnType(((CachePropVertexStep) t).returnsVertex() ? Gremlin.EntityType.VERTEX : Gremlin.EntityType.EDGE)
                    .setDirection(Gremlin.Direction.valueOf(((CachePropVertexStep) t).getDirection().name()))
                    .setRequiredProperties(((CachePropVertexStep) t).cacheProperties());
                List<String> edgeLabels = Arrays.asList(((CachePropVertexStep) t).getEdgeLabels());
                if (!edgeLabels.isEmpty()) {
                    edgeLabels.forEach(l -> builder.addEdgeLabels(Integer.valueOf(l)));
                }
                SnapshotIdFetcher fetcher = (SnapshotIdFetcher) conf.getProperty(PlanConfig.SNAPSHOT_ID_FETCHER);
                if (fetcher != null) {
                    builder.setSnapshotId(Gremlin.SnapShotId.newBuilder().setId(fetcher.getSnapshotId()));
                }
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.RepeatStep, new JobBuilderResource() {
            @Override
            public void buildJob(StepBuilder stepBuilder) {
                Step step = stepBuilder.getStep();
                Configuration conf = stepBuilder.getConf();
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                // add repeat_traversal
                List<Step> steps = ((Traversal.Admin) ((RepeatStep) step).getGlobalChildren().get(0)).getSteps();
                Traversal.Admin repeat = new DefaultTraversal();
                for (Step s : steps) {
                    repeat.addStep(s);
                }
                // add loops
                Traversal.Admin traversal = ((RepeatStep) step).getUntilTraversal();
                long times = 0;
                if (traversal instanceof LoopTraversal) {
                    times = ((LoopTraversal) traversal).getMaxLoops();
                }
                target.repeat((int) times, (JobBuilder) new TraversalTranslator((new TraversalBuilder(repeat)).setConf(conf)).translate());
            }
        });
        stepPlanMap.put(STEP.PathStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                return Gremlin.PathStep.newBuilder()
                    .build();
            }
        });
        stepPlanMap.put(STEP.PathFilterStep, new JobBuilderResource() {
            @Override
            protected void buildJob(StepBuilder stepBuilder) {
                Step t = stepBuilder.getStep();
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                target.filter(Gremlin.GremlinStep.newBuilder().setPathFilterStep(Gremlin.PathFilterStep.newBuilder()
                    .setHint(((PathFilterStep) t).isSimple() ? Gremlin.PathFilterStep.PathHint.SIMPLE : Gremlin.PathFilterStep.PathHint.CYCLIC))
                    .build().toByteString());
            }
        });
        stepPlanMap.put(STEP.RangeGlobalStep, new JobBuilderResource() {
            @Override
            public void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                Step step = stepBuilder.getStep();
                target.limit(true, (int) ((RangeGlobalStep) step).getHighRange());
            }
        });
        stepPlanMap.put(STEP.HasAnyStep, new JobBuilderResource() {
            @Override
            protected void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                target.limit(true, 1);
            }
        });
        stepPlanMap.put(STEP.SelectOneStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step step, Configuration conf) {
                Map.Entry<String, Traversal.Admin> selectOne = PlanUtils.getFirstEntry(PlanUtils.getSelectTraversalMap(step));
                // todo: hack way to implement select("a") temporarily
                if (selectOne.getValue() == null) {
                    IdMaker tagIdMaker = PlanUtils.getTagIdMaker(conf);
                    return Gremlin.SelectOneStepWithoutBy.newBuilder()
                        .setTag(Gremlin.StepTag.newBuilder().setTag((int) tagIdMaker.getId(selectOne.getKey())))
                        .build();
                } else {
                    return Gremlin.SelectStep.newBuilder()
                        .setPop(PlanUtils.convertFrom(((SelectOneStep) step).getPop()))
                        .addSelectKeys(TagKeyExtractorFactory.Select.extractFrom(selectOne.getKey(), selectOne.getValue(), false, conf))
                        .build();
                }
            }
        });
        stepPlanMap.put(STEP.SelectStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step step, Configuration conf) {
                Map<String, Traversal.Admin> selectTraversals = PlanUtils.getSelectTraversalMap(step);
                Gremlin.SelectStep.Builder builder = Gremlin.SelectStep.newBuilder().setPop(PlanUtils.convertFrom(((SelectStep) step).getPop()));
                selectTraversals.forEach((k, v) -> {
                    builder.addSelectKeys(TagKeyExtractorFactory.Select.extractFrom(k, v, false, conf));
                });
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.PropertyIdentityStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                Gremlin.IdentityStep.Builder builder = Gremlin.IdentityStep.newBuilder()
                    .setRequiredProperties(PlanUtils.convertFrom(((PropertyIdentityStep) t).getAttachProperties()));
                SnapshotIdFetcher fetcher = (SnapshotIdFetcher) conf.getProperty(PlanConfig.SNAPSHOT_ID_FETCHER);
                if (fetcher != null) {
                    builder.setSnapshotId(Gremlin.SnapShotId.newBuilder().setId(fetcher.getSnapshotId()));
                }
                return builder.build();
            }
        });
        stepPlanMap.put(STEP.OrderGlobalStep, new JobBuilderResource() {
            @Override
            public void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                Configuration conf = stepBuilder.getConf();
                Step step = stepBuilder.getStep();
                target.sortBy(true, Gremlin.GremlinStep.newBuilder()
                    .setOrderByStep(PlanUtils.constructFrom((ComparatorHolder) step, conf))
                    .build().toByteString());
            }
        });
        stepPlanMap.put(STEP.OrderGlobalLimitStep, new JobBuilderResource() {
            @Override
            public void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                Step step = stepBuilder.getStep();
                Configuration conf = stepBuilder.getConf();
                target.topBy(true, ((OrderGlobalLimitStep) step).getLimit(), Gremlin.GremlinStep.newBuilder()
                    .setOrderByStep(PlanUtils.constructFrom((ComparatorHolder) step, conf))
                    .build().toByteString());
            }
        });
        stepPlanMap.put(STEP.GroupCountStep, new JobBuilderResource() {
            @Override
            public void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                Step step = stepBuilder.getStep();
                Configuration conf = stepBuilder.getConf();
                Gremlin.GremlinStep.Builder builder = Gremlin.GremlinStep.newBuilder().setGroupByStep(PlanUtils.constructFrom(step, conf));
                stepBuilder.setJobBuilder(target.groupBy(true, builder.build().toByteString()));
            }
        });
        stepPlanMap.put(STEP.GroupStep, new JobBuilderResource() {
            @Override
            public void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                Step step = stepBuilder.getStep();
                Configuration conf = stepBuilder.getConf();
                Gremlin.GremlinStep.Builder builder = Gremlin.GremlinStep.newBuilder().setGroupByStep(PlanUtils.constructFrom(step, conf));
                stepBuilder.setJobBuilder(target.groupBy(true, builder.build().toByteString()));
            }
        });
        stepPlanMap.put(STEP.CountGlobalStep, new JobBuilderResource() {
            @Override
            public void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                stepBuilder.setJobBuilder(target.count(true));
            }
        });
        stepPlanMap.put(STEP.BySubTaskStep, new JobBuilderResource() {
            @Override
            protected void buildJob(StepBuilder stepBuilder) {
                Step t = stepBuilder.getStep();
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                Configuration conf = stepBuilder.getConf();
                Traversal.Admin subTraversal = ((BySubTaskStep) t).getBySubTraversal();
                BySubTaskStep.JoinerType type = ((BySubTaskStep) t).getJoiner();
                if (subTraversal != null) {
                    target.forkJoin(PlanUtils.getByJoiner(type).toByteString(),
                        (JobBuilder) new TraversalTranslator(new TraversalBuilder(subTraversal).setConf(conf)).translate());
                }
            }
        });
        stepPlanMap.put(STEP.UnionStep, new JobBuilderResource() {
            @Override
            protected void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                Step t = stepBuilder.getStep();
                Configuration conf = stepBuilder.getConf();
                target.union((List<JobBuilder>) ((UnionStep) t).getGlobalChildren().stream()
                    .map(k -> new TraversalTranslator(new TraversalBuilder((Traversal.Admin) k).setConf(conf)).translate())
                    .collect(Collectors.toList()));
            }
        });
        stepPlanMap.put(STEP.PathLocalCountStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                return Gremlin.PathLocalCountStep.newBuilder().build();
            }
        });
        stepPlanMap.put(STEP.IdentityStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                return Gremlin.IdentityStep.newBuilder()
                    .setRequiredProperties(PlanUtils.convertFrom(new ToFetchProperties(false, Collections.EMPTY_LIST)))
                    .build();
            }
        });
        stepPlanMap.put(STEP.TraversalMapStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                Traversal.Admin mapTraversal = (Traversal.Admin) ((TraversalMapStep) t).getLocalChildren().iterator().next();
                return Gremlin.SelectStep.newBuilder()
                    .addSelectKeys(TagKeyExtractorFactory.TraversalMap.extractFrom(mapTraversal))
                    .build();
            }
        });
        stepPlanMap.put(STEP.PropertiesStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                String[] properties = ((PropertiesStep) t).getPropertyKeys();
                boolean needAll = (properties == null || properties.length == 0) ? true : false;
                return Gremlin.PropertiesStep.newBuilder()
                    .setPropKeys(PlanUtils.convertFrom(new ToFetchProperties(needAll, Arrays.asList(properties))))
                    .build();
            }
        });
        stepPlanMap.put(STEP.EdgeVertexStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                // flat-map interface
                if (((EdgeVertexStep) t).getDirection() == Direction.BOTH) {
                    return Gremlin.EdgeBothVStep.newBuilder().build();
                } else {
                    return Gremlin.EdgeVertexStep.newBuilder()
                        .setEndpointOpt(Gremlin.EdgeVertexStep.EndpointOpt.valueOf(((EdgeVertexStep) t).getDirection().name()))
                        .build();
                }
            }
        });
        // merge {otherV, outV, inV} for integrated map interface
        stepPlanMap.put(STEP.EdgeOtherVertexStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                return Gremlin.EdgeVertexStep.newBuilder()
                    .setEndpointOpt(Gremlin.EdgeVertexStep.EndpointOpt.valueOf(PlanUtils.DIRECTION_OTHER))
                    .build();
            }
        });
        stepPlanMap.put(STEP.DedupGlobalStep, new JobBuilderResource() {
            @Override
            protected void buildJob(StepBuilder stepBuilder) {
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                target.dedup(true, Gremlin.GremlinStep.newBuilder()
                    .setDedupStep(Gremlin.DedupStep.newBuilder()
                        .setDedupType(Gremlin.DedupStep.DedupSetType.HashSet)).build().toByteString());
            }
        });
        stepPlanMap.put(STEP.UnfoldStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                return Gremlin.UnfoldStep.newBuilder().build();
            }
        });
        stepPlanMap.put(STEP.PhysicalPlanUnfoldStep, new JobBuilderResource() {
            @Override
            protected void buildJob(StepBuilder stepBuilder) {
                // todo: distinguish between group().unfold() and count().unfold()
                ReduceBuilder target = (ReduceBuilder) stepBuilder.getJobBuilder();
                stepBuilder.setJobBuilder(target.unfold(ByteString.EMPTY));
            }
        });
        stepPlanMap.put(STEP.TransformTraverserStep, new GremlinStepResource() {
            @Override
            protected Object getStepResource(Step t, Configuration conf) {
                return Gremlin.TransformTraverserStep.newBuilder().addTraverserRequirements(
                    Gremlin.TraverserRequirement.valueOf(((TransformTraverserStep) t).getRequirement().name())).build();
            }
        });
        stepPlanMap.put(STEP.IsStep, new JobBuilderResource() {
            @Override
            protected void buildJob(StepBuilder stepBuilder) {
                Step t = stepBuilder.getStep();
                JobBuilder target = (JobBuilder) stepBuilder.getJobBuilder();
                P p = ((IsStep) t).getPredicate();
                target.filter(Gremlin.GremlinStep.newBuilder().setIsStep(
                    Gremlin.IsStep.newBuilder().setSingle(FilterHelper.INSTANCE.valueComparePredicate(p.getValue(), p.getBiPredicate()))
                ).build().toByteString());
            }
        });
    }

    public static Optional<StepResource> getResourceConstructor(STEP step) {
        return Optional.ofNullable(stepPlanMap.get(step));
    }

    private final static Map<STEP, StepMetaRequiredInfo> stepMetaInfoMap;

    static {
        stepMetaInfoMap = new HashMap<>();
        // examples
        stepMetaInfoMap.put(STEP.PathFilterStep, StepMetaRequiredInfo.Builder.newBuilder()
            .setNeedPathHistory(true).build()
        );
        stepMetaInfoMap.put(STEP.PropertiesStep, StepMetaRequiredInfo.Builder.newBuilder()
            .setTraverserMapFunc((stepEle) -> new TraverserElement(new CompositeObject(String.class)))
            .setExtractor(new PropertyExtractor() {
                @Override
                public ToFetchProperties extractProperties(Step step) {
                    String[] properties = ((PropertiesStep) step).getPropertyKeys();
                    boolean needAll = (properties == null || properties.length == 0) ? true : false;
                    return new ToFetchProperties(needAll, Arrays.asList(properties));
                }
            }).build()
        );
        stepMetaInfoMap.put(STEP.VertexStep, StepMetaRequiredInfo.Builder.newBuilder()
            .setTraverserMapFunc(
                (stepEle) -> {
                    VertexStep step = (VertexStep) stepEle.getStep();
                    CompositeObject returnObj = step.returnsVertex() ? new CompositeObject(new Vertex()) : new CompositeObject(new Edge());
                    return new TraverserElement(returnObj);
                }
            ).build()
        );
    }

    public static Optional<StepMetaRequiredInfo> getStepMetaRequiredInfo(STEP step) {
        return Optional.ofNullable(stepMetaInfoMap.get(step));
    }
}
