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

package com.alibaba.graphscope.gremlin.transform;

import com.alibaba.graphscope.common.exception.OpArgIllegalException;
import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.MatchSentence;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.antlr4.GremlinAntlrToJava;
import com.alibaba.graphscope.gremlin.plugin.step.ExprStep;
import com.alibaba.graphscope.gremlin.plugin.step.PathExpandStep;
import com.alibaba.graphscope.gremlin.plugin.step.ScanFusionStep;
import com.alibaba.graphscope.gremlin.transform.alias.AliasArg;
import com.alibaba.graphscope.gremlin.transform.alias.AliasManager;
import com.alibaba.graphscope.gremlin.transform.alias.AliasPrefixType;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ColumnTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum StepTransformFactory implements Function<Step, InterOpBase> {
    GRAPH_STEP {
        @Override
        public InterOpBase apply(Step step) {
            GraphStep graphStep = (GraphStep) step;
            ScanFusionOp op = new ScanFusionOp();
            op.setScanOpt(new OpArg<>(graphStep, SCAN_OPT));

            if (graphStep.getIds().length > 0) {
                op.setIds(new OpArg<>(graphStep, CONST_IDS_FROM_STEP));
            }
            return op;
        }
    },
    SCAN_FUSION_STEP {
        @Override
        public InterOpBase apply(Step step) {
            ScanFusionStep scanFusionStep = (ScanFusionStep) step;
            ScanFusionOp op = new ScanFusionOp();

            op.setScanOpt(new OpArg<>(scanFusionStep, SCAN_OPT));

            // set global ids
            if (scanFusionStep.getIds() != null && scanFusionStep.getIds().length > 0) {
                op.setIds(new OpArg(scanFusionStep, CONST_IDS_FROM_STEP));
            }

            QueryParams params = new QueryParams();
            // set labels
            List<String> labels = scanFusionStep.getGraphLabels();
            for (String label : labels) {
                params.addTable(ArgUtils.asFfiTag(label));
            }
            List<HasContainer> containers = scanFusionStep.getHasContainers();
            if (!containers.isEmpty()) {
                String predicate = PredicateExprTransformFactory.HAS_STEP.apply(scanFusionStep);
                params.setPredicate(predicate);
            }
            op.setParams(params);

            return op;
        }
    },
    HAS_STEP {
        @Override
        public InterOpBase apply(Step step) {
            SelectOp op = new SelectOp();
            List containers = ((HasStep) step).getHasContainers();
            // add corner judgement
            if (!containers.isEmpty()) {
                op.setPredicate(new OpArg(step, PredicateExprTransformFactory.HAS_STEP));
            }
            return op;
        }
    },
    IS_STEP {
        @Override
        public InterOpBase apply(Step step) {
            SelectOp op = new SelectOp();
            op.setPredicate(new OpArg(step, PredicateExprTransformFactory.IS_STEP));
            return op;
        }
    },
    VERTEX_STEP {
        @Override
        public InterOpBase apply(Step step) {
            ExpandOp op = new ExpandOp();
            op.setDirection(
                    new OpArg<>(
                            (VertexStep) step,
                            (VertexStep s1) -> {
                                Direction direction = s1.getDirection();
                                switch (direction) {
                                    case IN:
                                        return FfiDirection.In;
                                    case BOTH:
                                        return FfiDirection.Both;
                                    case OUT:
                                        return FfiDirection.Out;
                                    default:
                                        throw new OpArgIllegalException(
                                                OpArgIllegalException.Cause.INVALID_TYPE,
                                                "invalid direction type");
                                }
                            }));
            op.setEdgeOpt(
                    new OpArg<>(
                            (VertexStep) step,
                            (VertexStep s1) -> {
                                if (s1.returnsEdge()) {
                                    return Boolean.valueOf(true);
                                } else {
                                    return Boolean.valueOf(false);
                                }
                            }));

            QueryParams params = new QueryParams();
            String[] labels = ((VertexStep) step).getEdgeLabels();
            if (labels.length > 0) {
                for (String label : labels) {
                    params.addTable(ArgUtils.asFfiTag(label));
                }
            }
            op.setParams(params);
            return op;
        }
    },
    LIMIT_STEP {
        @Override
        public InterOpBase apply(Step step) {
            RangeGlobalStep limitStep = (RangeGlobalStep) step;
            int lower = (int) limitStep.getLowRange() + 1;
            int upper = (int) limitStep.getHighRange() + 1;
            LimitOp op = new LimitOp();
            op.setLower(new OpArg(Integer.valueOf(lower)));
            op.setUpper(new OpArg(Integer.valueOf(upper)));
            return op;
        }
    },
    VALUE_MAP_STEP {
        @Override
        public InterOpBase apply(Step step) {
            PropertyMapStep valueMapStep = (PropertyMapStep) step;
            ProjectOp op = new ProjectOp();
            String expr =
                    TraversalParentTransformFactory.PROJECT_BY_STEP
                            .getSubTraversalAsExpr((new ExprArg()).addStep(valueMapStep))
                            .getSingleExpr()
                            .get();
            op.setExprWithAlias(
                    new OpArg<>(
                            expr,
                            (String expr1) -> {
                                FfiAlias.ByValue alias = ArgUtils.asFfiNoneAlias();
                                return Arrays.asList(Pair.with(expr1, alias));
                            }));
            return op;
        }
    },
    VALUES_STEP {
        @Override
        public InterOpBase apply(Step step) {
            PropertiesStep valuesStep = (PropertiesStep) step;
            ProjectOp op = new ProjectOp();
            String expr =
                    TraversalParentTransformFactory.PROJECT_BY_STEP
                            .getSubTraversalAsExpr((new ExprArg()).addStep(valuesStep))
                            .getSingleExpr()
                            .get();
            op.setExprWithAlias(
                    new OpArg<>(
                            expr,
                            (String expr1) -> {
                                FfiAlias.ByValue alias = ArgUtils.asFfiNoneAlias();
                                return Arrays.asList(Pair.with(expr1, alias));
                            }));
            return op;
        }
    },
    DEDUP_STEP {
        @Override
        public InterOpBase apply(Step step) {
            DedupGlobalStep dedupStep = (DedupGlobalStep) step;
            Map<String, Traversal.Admin> tagTraversals = getDedupTagTraversal(dedupStep);
            DedupOp op = new DedupOp();
            op.setDedupKeys(
                    new OpArg<>(
                            tagTraversals,
                            (Map<String, Traversal.Admin> map) -> {
                                if (tagTraversals.isEmpty()) { // only support dedup()
                                    return Collections.singletonList(ArgUtils.asFfiNoneVar());
                                } else {
                                    throw new OpArgIllegalException(
                                            OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                                            "supported pattern is [dedup()]");
                                }
                            }));
            return op;
        }

        // dedup("a").by("name"): a -> "name"
        private Map<String, Traversal.Admin> getDedupTagTraversal(DedupGlobalStep step) {
            Set<String> dedupTags = step.getScopeKeys();
            List<Traversal.Admin> dedupTraversals = step.getLocalChildren();
            Map<String, Traversal.Admin> tagTraversals = new HashMap<>();
            if (dedupTags.isEmpty() && dedupTraversals.isEmpty()) {
                return tagTraversals;
            }
            if (dedupTags.isEmpty()) {
                dedupTags = new HashSet<>();
                // set as head
                dedupTags.add("");
            }
            dedupTags.forEach(
                    k -> {
                        Traversal.Admin dedupTraversal =
                                dedupTraversals.isEmpty() ? null : dedupTraversals.get(0);
                        tagTraversals.put(k, dedupTraversal);
                    });
            tagTraversals.entrySet().removeIf(e -> e.getKey().equals("") && e.getValue() == null);
            return tagTraversals;
        }
    },
    COUNT_STEP {
        @Override
        public InterOpBase apply(Step step) {
            CountGlobalStep countStep = (CountGlobalStep) step;
            GroupOp op = new GroupOp();
            op.setGroupByKeys(new OpArg(Collections.emptyList()));
            op.setGroupByValues(new OpArg(getCountAgg(countStep)));
            return op;
        }

        private List<ArgAggFn> getCountAgg(CountGlobalStep step1) {
            int stepIdx = TraversalHelper.stepIndex(step1, step1.getTraversal());
            FfiAlias.ByValue valueAlias =
                    AliasManager.getFfiAlias(new AliasArg(AliasPrefixType.GROUP_VALUES, stepIdx));
            // count().as("a"), "a" is the alias of group value
            if (!step1.getLabels().isEmpty()) {
                String label = (String) step1.getLabels().iterator().next();
                valueAlias = ArgUtils.asFfiAlias(label, true);
                step1.removeLabel(label);
            }
            ArgAggFn countAgg = new ArgAggFn(FfiAggOpt.Count, valueAlias);
            return Collections.singletonList(countAgg);
        }
    },
    PATH_EXPAND_STEP {
        @Override
        public InterOpBase apply(Step step) {
            PathExpandOp op = new PathExpandOp((ExpandOp) VERTEX_STEP.apply(step));
            PathExpandStep pathStep = (PathExpandStep) step;
            op.setLower(new OpArg(Integer.valueOf(pathStep.getLower())));
            op.setUpper(new OpArg(Integer.valueOf(pathStep.getUpper())));
            return op;
        }
    },
    EDGE_VERTEX_STEP {
        @Override
        public InterOpBase apply(Step step) {
            EdgeVertexStep vertexStep = (EdgeVertexStep) step;
            GetVOp op = new GetVOp();
            op.setGetVOpt(
                    new OpArg<>(
                            vertexStep,
                            (EdgeVertexStep edgeVertexStep) -> {
                                Direction direction = edgeVertexStep.getDirection();
                                switch (direction) {
                                    case OUT:
                                        return FfiVOpt.Start;
                                    case IN:
                                        return FfiVOpt.End;
                                    case BOTH:
                                    default:
                                        throw new OpArgIllegalException(
                                                OpArgIllegalException.Cause.INVALID_TYPE,
                                                direction + " cannot be converted to FfiVOpt");
                                }
                            }));
            op.setParams(new QueryParams());
            return op;
        }
    },
    EDGE_OTHER_STEP {
        @Override
        public InterOpBase apply(Step step) {
            EdgeOtherVertexStep otherStep = (EdgeOtherVertexStep) step;
            GetVOp op = new GetVOp();
            op.setGetVOpt(
                    new OpArg<>(otherStep, (EdgeOtherVertexStep otherStep1) -> FfiVOpt.Other));
            return op;
        }
    },
    WHERE_START_STEP {
        @Override
        public InterOpBase apply(Step step) {
            WhereTraversalStep.WhereStartStep startStep = (WhereTraversalStep.WhereStartStep) step;
            String selectKey = (String) startStep.getScopeKeys().iterator().next();

            ProjectOp op = new ProjectOp();
            op.setExprWithAlias(
                    new OpArg<>(
                            selectKey,
                            (String key) -> {
                                String expr = "@" + selectKey;
                                FfiAlias.ByValue alias = ArgUtils.asFfiNoneAlias();
                                return Collections.singletonList(Pair.with(expr, alias));
                            }));
            return op;
        }
    },
    WHERE_END_STEP {
        @Override
        public InterOpBase apply(Step step) {
            WhereTraversalStep.WhereEndStep endStep = (WhereTraversalStep.WhereEndStep) step;
            SelectOp selectOp = new SelectOp();
            selectOp.setPredicate(new OpArg(endStep, PredicateExprTransformFactory.WHERE_END_STEP));
            return selectOp;
        }
    },
    UNION_STEP {
        @Override
        public InterOpBase apply(Step step) {
            UnionOp unionOp = new UnionOp();
            unionOp.setSubOpCollectionList(
                    new OpArg<>(
                            (UnionStep) step,
                            (UnionStep unionStep) -> {
                                List<Traversal.Admin> subTraversals = unionStep.getGlobalChildren();
                                return subTraversals.stream()
                                        .filter(k -> k != null)
                                        .map(k -> (new InterOpCollectionBuilder(k)).build())
                                        .collect(Collectors.toList());
                            }));
            return unionOp;
        }
    },
    TRAVERSAL_MAP_STEP {
        @Override
        public InterOpBase apply(Step step) {
            TraversalMapStep mapStep = (TraversalMapStep) step;
            List<Traversal.Admin> mapTraversals = mapStep.getLocalChildren();
            if (mapTraversals.size() != 1 || !(mapTraversals.get(0) instanceof ColumnTraversal)) {
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                        "only support select(keys/values)");
            }
            Column column = ((ColumnTraversal) mapTraversals.get(0)).getColumn();
            switch (column) {
                case keys:
                    String key = getMapKey(mapStep);
                    SelectOneStep keySelect = new SelectOneStep(step.getTraversal(), Pop.last, key);
                    TraversalHelper.copyLabels(mapStep, keySelect, false);
                    return TraversalParentTransformFactory.PROJECT_BY_STEP.apply(keySelect).get(0);
                case values:
                    String value = getMapValue(mapStep);
                    SelectOneStep valueSelect =
                            new SelectOneStep(step.getTraversal(), Pop.last, value);
                    TraversalHelper.copyLabels(mapStep, valueSelect, false);
                    return TraversalParentTransformFactory.PROJECT_BY_STEP
                            .apply(valueSelect)
                            .get(0);
                default:
                    throw new OpArgIllegalException(
                            OpArgIllegalException.Cause.INVALID_TYPE,
                            column.name() + " is invalid");
            }
        }

        private String getMapKey(TraversalMapStep step) {
            Step groupStep = getPreviousGroupStep(step);
            int stepIdx = TraversalHelper.stepIndex(groupStep, groupStep.getTraversal());
            FfiAlias.ByValue keyAlias =
                    AliasManager.getFfiAlias(new AliasArg(AliasPrefixType.GROUP_KEYS, stepIdx));
            return keyAlias.alias.name;
        }

        private String getMapValue(TraversalMapStep step) {
            Step groupStep = getPreviousGroupStep(step);
            int stepIdx = TraversalHelper.stepIndex(groupStep, groupStep.getTraversal());
            FfiAlias.ByValue valueAlias =
                    AliasManager.getFfiAlias(new AliasArg(AliasPrefixType.GROUP_VALUES, stepIdx));
            return valueAlias.alias.name;
        }

        private Step getPreviousGroupStep(Step step) {
            Step previous = step.getPreviousStep();
            while (!(previous instanceof EmptyStep
                    || previous instanceof GroupStep
                    || previous instanceof GroupCountStep)) {
                previous = previous.getPreviousStep();
            }
            if (!(previous instanceof EmptyStep)) {
                return previous;
            }
            TraversalParent parent = step.getTraversal().getParent();
            if (!(parent instanceof EmptyStep)) {
                return getPreviousGroupStep(parent.asStep());
            }
            throw new OpArgIllegalException(
                    OpArgIllegalException.Cause.INVALID_TYPE,
                    "select keys or values should follow a group");
        }
    },
    MATCH_STEP {
        @Override
        public InterOpBase apply(Step step) {
            MatchStep matchStep = (MatchStep) step;
            List<MatchSentence> sentences = getSentences(matchStep);
            MatchOp matchOp = new MatchOp();
            matchOp.setSentences(new OpArg(sentences));
            return matchOp;
        }

        private List<MatchSentence> getSentences(MatchStep matchStep) {
            List<Traversal.Admin> matchTraversals = matchStep.getGlobalChildren();
            List<MatchSentence> sentences = new ArrayList<>();
            matchTraversals.forEach(
                    traversal -> {
                        List<Step> binderSteps = new ArrayList<>();
                        Optional<String> startTag = Optional.empty();
                        Optional<String> endTag = Optional.empty();
                        FfiJoinKind joinKind = FfiJoinKind.Inner;
                        for (Object o : traversal.getSteps()) {
                            Step s = (Step) o;
                            if (s instanceof MatchStep.MatchStartStep) { // match(__.as("a")...)
                                Optional<String> selectKey =
                                        ((MatchStep.MatchStartStep) s).getSelectKey();
                                if (!startTag.isPresent() && selectKey.isPresent()) {
                                    startTag = selectKey;
                                }
                            } else if (s instanceof MatchStep.MatchEndStep) { // match(...as("b"))
                                Optional<String> matchKey =
                                        ((MatchStep.MatchEndStep) s).getMatchKey();
                                if (!endTag.isPresent() && matchKey.isPresent()) {
                                    endTag = matchKey;
                                }
                            } else if (s instanceof WhereTraversalStep
                                    && binderSteps.isEmpty()) { // where(__.as("a")...) or not(...)
                                List<Traversal.Admin> children =
                                        ((WhereTraversalStep) s).getLocalChildren();
                                Traversal.Admin whereTraversal =
                                        children.isEmpty() ? null : children.get(0);
                                // not(as("a").out().as("b"))
                                if (whereTraversal != null
                                        && whereTraversal.getSteps().size() == 1
                                        && whereTraversal.getStartStep() instanceof NotStep) {
                                    NotStep notStep = (NotStep) whereTraversal.getStartStep();
                                    List<Traversal.Admin> notChildren = notStep.getLocalChildren();
                                    whereTraversal =
                                            (notChildren.isEmpty()) ? null : notChildren.get(0);
                                    joinKind = FfiJoinKind.Anti;
                                } else { // where(as("a").out().as("b"))
                                    joinKind = FfiJoinKind.Semi;
                                }
                                if (whereTraversal != null) {
                                    for (Object o1 : whereTraversal.getSteps()) {
                                        Step s1 = (Step) o1;
                                        if (s1
                                                instanceof
                                                WhereTraversalStep
                                                        .WhereStartStep) { // not(__.as("a")...)
                                            Set<String> scopeKeys;
                                            if (!startTag.isPresent()
                                                    && !(scopeKeys =
                                                                    ((WhereTraversalStep
                                                                                            .WhereStartStep)
                                                                                    s1)
                                                                            .getScopeKeys())
                                                            .isEmpty()) {
                                                startTag = Optional.of(scopeKeys.iterator().next());
                                            }
                                        } else if (s1
                                                instanceof
                                                WhereTraversalStep
                                                        .WhereEndStep) { // not(....as("b"))
                                            Set<String> scopeKeys;
                                            if (!endTag.isPresent()
                                                    && !(scopeKeys =
                                                                    ((WhereTraversalStep
                                                                                            .WhereEndStep)
                                                                                    s1)
                                                                            .getScopeKeys())
                                                            .isEmpty()) {
                                                endTag = Optional.of(scopeKeys.iterator().next());
                                            }
                                        } else if (isValidBinderStep(s1)) {
                                            binderSteps.add(s1);
                                        } else {
                                            throw new OpArgIllegalException(
                                                    OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                                                    s1.getClass() + " is unsupported yet in match");
                                        }
                                    }
                                }
                            } else if (isValidBinderStep(s)) {
                                binderSteps.add(s);
                            } else {
                                throw new OpArgIllegalException(
                                        OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                                        s.getClass() + " is unsupported yet in match");
                            }
                        }
                        if (!startTag.isPresent() || !endTag.isPresent()) {
                            throw new OpArgIllegalException(
                                    OpArgIllegalException.Cause.INVALID_TYPE,
                                    "startTag or endTag not exist in match");
                        }
                        Traversal binderTraversal = GremlinAntlrToJava.getTraversalSupplier().get();
                        binderSteps.forEach(
                                b -> {
                                    binderTraversal.asAdmin().addStep(b);
                                });
                        InterOpCollection ops =
                                (new InterOpCollectionBuilder(binderTraversal)).build();
                        sentences.add(
                                new MatchSentence(startTag.get(), endTag.get(), ops, joinKind));
                    });
            return sentences;
        }

        private boolean isValidBinderStep(Step step) {
            return step instanceof VertexStep // in()/out()/both()/inE()/outE()/bothE()
                    || step instanceof PathExpandStep // out/in/both('1..5', 'knows')
                    || step instanceof EdgeOtherVertexStep // otherV()
                    || step instanceof EdgeVertexStep; // inV()/outV()/endV()(todo)
        }
    },

    EXPR_STEP {
        @Override
        public InterOpBase apply(Step step) {
            ExprStep exprStep = (ExprStep) step;
            switch (exprStep.getType()) {
                case PROJECTION:
                    ProjectOp projectOp = new ProjectOp();
                    projectOp.setExprWithAlias(
                            new OpArg<>(
                                    exprStep.getExpr(),
                                    (String expr) ->
                                            Arrays.asList(
                                                    Pair.with(expr, ArgUtils.asFfiNoneAlias()))));
                    return projectOp;
                case FILTER:
                default:
                    SelectOp selectOp = new SelectOp();
                    selectOp.setPredicate(new OpArg(exprStep.getExpr()));
                    return selectOp;
            }
        }
    };

    protected Function<GraphStep, FfiScanOpt> SCAN_OPT =
            (GraphStep s1) -> {
                if (s1.returnsVertex()) return FfiScanOpt.Entity;
                else return FfiScanOpt.Relation;
            };

    protected Function<GraphStep, List<FfiConst.ByValue>> CONST_IDS_FROM_STEP =
            (GraphStep s1) ->
                    Arrays.stream(s1.getIds())
                            .map(
                                    (id) -> {
                                        if (id instanceof Integer) {
                                            return ArgUtils.asFfiConst((Integer) id);
                                        } else if (id instanceof Long) {
                                            return ArgUtils.asFfiConst((Long) id);
                                        } else {
                                            throw new OpArgIllegalException(
                                                    OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                                                    "unimplemented yet");
                                        }
                                    })
                            .collect(Collectors.toList());

    protected Function<ScanFusionStep, List<FfiNameOrId.ByValue>> LABELS_FROM_STEP =
            (ScanFusionStep step) -> {
                List<String> labels = step.getGraphLabels();
                return labels.stream().map(k -> ArgUtils.asFfiTag(k)).collect(Collectors.toList());
            };
}
