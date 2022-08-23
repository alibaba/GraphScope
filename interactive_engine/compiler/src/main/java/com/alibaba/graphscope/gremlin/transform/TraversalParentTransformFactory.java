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
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.Utils;
import com.alibaba.graphscope.gremlin.antlr4.GremlinAntlrToJava;
import com.alibaba.graphscope.gremlin.transform.alias.AliasArg;
import com.alibaba.graphscope.gremlin.transform.alias.AliasManager;
import com.alibaba.graphscope.gremlin.transform.alias.AliasPrefixType;

import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.javatuples.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum TraversalParentTransformFactory implements TraversalParentTransform {
    // select("a").by("name") -> [ProjectOp("@a.name")]
    // select("a").by(out().count()) -> [ApplyOp(select("a").out().count()).as("a_apply"),
    // ProjectOp("@a_apply")]
    // select("a", "b").by("name").by(out().count()) ->
    // [ApplyOp(select("b").out().count()).as("b_apply"), ProjectOp("@a.name", "@b_apply")]
    PROJECT_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            List<InterOpBase> interOpList = new ArrayList<>();
            Map<String, Traversal.Admin> byTraversals = getProjectTraversals(parent);
            List<Pair<String, FfiAlias.ByValue>> projectExprWithAlias = new ArrayList<>();
            int stepIdx =
                    TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            int subId = 0;
            ExprResult exprRes =
                    getSubTraversalAsExpr(
                            (new ExprArg(Collections.singletonList(parent.asStep()))));
            for (Map.Entry<String, Traversal.Admin> entry : byTraversals.entrySet()) {
                String k = entry.getKey();
                Traversal.Admin v = entry.getValue();
                String expr;
                Optional<String> exprOpt = exprRes.getTagExpr(k);
                if (exprOpt.isPresent()) {
                    expr = exprOpt.get();
                } else { // use apply
                    ApplyOp applyOp = new ApplyOp();
                    applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                    // put select("") in apply
                    Traversal copy = GremlinAntlrToJava.getTraversalSupplier().get();
                    // copy steps in by(..) to apply
                    copy.asAdmin().addStep(new SelectOneStep(copy.asAdmin(), Pop.last, k));
                    v.asAdmin().getSteps().forEach(s -> copy.asAdmin().addStep((Step) s));
                    applyOp.setSubOpCollection(
                            new OpArg(new InterOpCollectionBuilder(copy).build(applyOp)));
                    // column key of apply result
                    FfiAlias.ByValue applyAlias =
                            AliasManager.getFfiAlias(
                                    new AliasArg(AliasPrefixType.PROJECT_TAG, k, stepIdx, subId));
                    applyOp.setAlias(new OpArg(applyAlias, Function.identity()));
                    interOpList.add(applyOp);
                    String aliasName = applyAlias.alias.name;
                    expr = "@" + aliasName;
                }
                FfiAlias.ByValue alias =
                        AliasManager.getFfiAlias(
                                new AliasArg(AliasPrefixType.PROJECT_TAG, k, stepIdx, subId));
                projectExprWithAlias.add(Pair.with(expr, alias));
                ++subId;
            }
            // optimize: if there is only one expression, alias with NONE
            if (projectExprWithAlias.size() == 1) {
                Pair single = projectExprWithAlias.get(0);
                projectExprWithAlias.set(0, single.setAt1(ArgUtils.asNoneAlias()));
            }
            ProjectOp op = new ProjectOp();
            op.setExprWithAlias(new OpArg(projectExprWithAlias));
            interOpList.add(op);
            return interOpList;
        }
    },
    DEDUP_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            DedupGlobalStep dedupStep = (DedupGlobalStep) parent;
            List<InterOpBase> interOpList = new ArrayList<>();
            List<FfiVariable.ByValue> dedupVars = new ArrayList<>();
            int stepIdx =
                    TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            int subId = 0;
            ExprResult exprRes =
                    getSubTraversalAsExpr(
                            (new ExprArg(Collections.singletonList(parent.asStep()))));
            Set<String> keys =
                    dedupStep.getScopeKeys().isEmpty()
                            ? Collections.singleton("")
                            : dedupStep.getScopeKeys();
            Traversal.Admin byTraversal =
                    dedupStep.getLocalChildren().isEmpty()
                            ? new IdentityTraversal()
                            : (Traversal.Admin) dedupStep.getLocalChildren().get(0);
            for (String k : keys) {
                Optional<String> exprOpt = exprRes.getTagExpr(k);
                String expr;
                if (exprOpt.isPresent()) { // dedup() or dedup().by('name') or dedup('a').by('name')
                    expr = exprOpt.get();
                } else { // dedup(..).by(out().count())
                    ApplyOp applyOp = new ApplyOp();
                    applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                    Traversal copy = GremlinAntlrToJava.getTraversalSupplier().get();
                    // put select("") in apply
                    if (!StringUtils.isEmpty(k)) { // dedup('a').by(out().count())
                        copy.asAdmin().addStep(new SelectOneStep(copy.asAdmin(), Pop.last, k));
                    }
                    // copy steps in by(..) to apply
                    byTraversal.getSteps().forEach(s -> copy.asAdmin().addStep((Step) s));
                    applyOp.setSubOpCollection(
                            new OpArg(new InterOpCollectionBuilder(copy).build(applyOp)));
                    FfiAlias.ByValue applyAlias =
                            AliasManager.getFfiAlias(
                                    new AliasArg(AliasPrefixType.DEFAULT, stepIdx, subId));
                    applyOp.setAlias(new OpArg(applyAlias));
                    interOpList.add(applyOp);
                    String applyAliasName = applyAlias.alias.name;
                    expr = "@" + applyAliasName;
                }
                dedupVars.add(getExpressionAsVar(expr));
            }
            DedupOp dedupOp = new DedupOp();
            dedupOp.setDedupKeys(new OpArg(dedupVars));
            interOpList.add(dedupOp);
            return interOpList;
        }
    },
    // order().by("name"), order().by(values("name")) -> [OrderOp("@.name")]
    // order().by(valueMap("name")) -> can not convert to FfiVariable with valueMap
    // order().by(select("a").by("name")), order().by(select("a").by(values("name"))) ->
    // OrderOp("@a.name")
    // order().by(out().count) -> [ApplyOp(out().count()).as("order_1_apply"),
    // OrderOp("@order_1_apply")]
    // order().by("name").by(out().count) -> [ApplyOp(out().count()).as("order_2_apply"),
    // OrderOp("@.name", "@order_2_apply")]
    ORDER_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            OrderGlobalStep orderStep = (OrderGlobalStep) parent;
            List<InterOpBase> interOpList = new ArrayList<>();
            List<Pair<String, FfiOrderOpt>> exprWithOrderList = new ArrayList<>();
            List<Pair> comparators = orderStep.getComparators();
            int stepIdx =
                    TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            for (int i = 0; i < comparators.size(); ++i) {
                Pair pair = comparators.get(i);
                Traversal.Admin admin = (Traversal.Admin) pair.getValue0();
                FfiOrderOpt orderOpt = getFfiOrderOpt((Order) pair.getValue1());
                ExprResult exprRes = getSubTraversalAsExpr(new ExprArg(admin));
                // i.e. order().by(values("name"))
                if (exprRes.isExprPattern()) {
                    List<String> exprs = exprRes.getExprs();
                    exprs.forEach(
                            k -> {
                                exprWithOrderList.add(Pair.with(k, orderOpt));
                            });
                } else { // use apply, i.e. order().by(out().count())
                    ApplyOp applyOp = new ApplyOp();
                    applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                    applyOp.setSubOpCollection(
                            new OpArg(new InterOpCollectionBuilder(admin).build(applyOp)));
                    FfiAlias.ByValue applyAlias =
                            AliasManager.getFfiAlias(
                                    new AliasArg(AliasPrefixType.DEFAULT, stepIdx, i));
                    applyOp.setAlias(new OpArg(applyAlias, Function.identity()));
                    interOpList.add(applyOp);
                    String aliasName = applyAlias.alias.name;
                    exprWithOrderList.add(Pair.with("@" + aliasName, orderOpt));
                }
            }
            OrderOp orderOp = new OrderOp();
            List varWithOrder =
                    exprWithOrderList.stream()
                            .map(
                                    k -> {
                                        String expr = (String) ((Pair) k).getValue0();
                                        FfiVariable.ByValue var = getExpressionAsVar(expr);
                                        return ((Pair) k).setAt0(var);
                                    })
                            .collect(Collectors.toList());
            orderOp.setOrderVarWithOrder(new OpArg(varWithOrder));
            interOpList.add(orderOp);
            return interOpList;
        }

        private FfiOrderOpt getFfiOrderOpt(Order order) {
            switch (order) {
                case asc:
                    return FfiOrderOpt.Asc;
                case desc:
                    return FfiOrderOpt.Desc;
                case shuffle:
                    return FfiOrderOpt.Shuffle;
                default:
                    throw new OpArgIllegalException(
                            OpArgIllegalException.Cause.INVALID_TYPE, "invalid order type");
            }
        }
    },
    GROUP_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            List<InterOpBase> interOpList = new ArrayList<>();
            // handle group key by
            Traversal.Admin groupKeyTraversal = getKeyTraversal(parent);
            List<String> groupKeyExprs = new ArrayList<>();
            int stepIdx =
                    TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            ExprResult exprRes = getSubTraversalAsExpr(new ExprArg(groupKeyTraversal));
            // i.e. group().by("name")
            if (exprRes.isExprPattern()) {
                groupKeyExprs.addAll(exprRes.getExprs());
            } else { // use apply, i.e. group().by(out().count())
                ApplyOp applyOp = new ApplyOp();
                applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                applyOp.setSubOpCollection(
                        new OpArg(new InterOpCollectionBuilder(groupKeyTraversal).build(applyOp)));
                FfiAlias.ByValue applyAlias =
                        AliasManager.getFfiAlias(new AliasArg(AliasPrefixType.GROUP_KEYS, stepIdx));
                applyOp.setAlias(new OpArg(applyAlias, Function.identity()));
                interOpList.add(applyOp);
                String aliasName = applyAlias.alias.name;
                groupKeyExprs.add("@" + aliasName);
            }
            List<Pair<FfiVariable.ByValue, FfiAlias.ByValue>> groupKeyVarWithAlias =
                    new ArrayList<>();
            for (int i = 0; i < groupKeyExprs.size(); ++i) {
                String expr = groupKeyExprs.get(i);
                FfiVariable.ByValue groupKeyVar = getExpressionAsVar(expr);
                FfiAlias.ByValue groupKeyAlias =
                        AliasManager.getFfiAlias(
                                new AliasArg(AliasPrefixType.GROUP_KEYS, stepIdx, i));
                groupKeyVarWithAlias.add(Pair.with(groupKeyVar, groupKeyAlias));
            }
            Step endStep;
            // group().by(values("name").as("a")), "a" is the query given alias of group key
            if (groupKeyTraversal != null
                    && !((endStep = groupKeyTraversal.getEndStep()) instanceof EmptyStep)
                    && !endStep.getLabels().isEmpty()) {
                String queryAlias = (String) endStep.getLabels().iterator().next();
                if (groupKeyVarWithAlias.size() == 1) {
                    Pair newPair =
                            groupKeyVarWithAlias.get(0).setAt1(ArgUtils.asAlias(queryAlias, true));
                    groupKeyVarWithAlias.set(0, newPair);
                } else if (groupKeyVarWithAlias.size() > 1) {
                    throw new OpArgIllegalException(
                            OpArgIllegalException.Cause.INVALID_TYPE,
                            "the query given alias cannot apply to multiple columns");
                }
            }
            GroupOp groupOp = new GroupOp();
            groupOp.setGroupByKeys(new OpArg(groupKeyVarWithAlias));
            // handle group value by
            groupOp.setGroupByValues(new OpArg(getGroupValueAsAggFn(parent)));
            interOpList.add(groupOp);
            return interOpList;
        }

        private Traversal.Admin getKeyTraversal(TraversalParent step) {
            if (step instanceof GroupStep) {
                GroupStep groupStep = (GroupStep) step;
                return groupStep.getKeyTraversal();
            } else if (step instanceof GroupCountStep) {
                GroupCountStep groupStep = (GroupCountStep) step;
                List<Traversal.Admin> keyTraversals = groupStep.getLocalChildren();
                return (keyTraversals.isEmpty()) ? null : keyTraversals.get(0);
            } else {
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.INVALID_TYPE,
                        "cannot get key traversal from " + step.getClass());
            }
        }

        private Traversal.Admin getValueTraversal(TraversalParent step) {
            if (step instanceof GroupStep) {
                GroupStep groupStep = (GroupStep) step;
                return groupStep.getValueTraversal();
            } else if (step instanceof GroupCountStep) {
                Traversal.Admin countTraversal = new DefaultTraversal();
                countTraversal.addStep(new CountGlobalStep(countTraversal));
                return countTraversal;
            } else {
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.INVALID_TYPE,
                        "cannot get value traversal from " + step.getClass());
            }
        }

        public List<ArgAggFn> getGroupValueAsAggFn(TraversalParent parent) {
            Traversal.Admin admin = getValueTraversal(parent);
            ArgAggFn aggFn;
            int stepIdx =
                    TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            Step endStep;
            if (admin == null || (endStep = admin.getEndStep()) == null) {
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.INVALID_TYPE,
                        "default is [FoldStep], null end step is invalid");
            }
            aggFn = getAggFn(endStep, stepIdx);
            // handle with CountDistinct and ToSet
            // specifically, variables from dedup will be treated as variables of aggregate
            // functions
            // i.e. group..by(dedup("a").by("name").count()) -> AggFn { FfiVariable<@a.name>
            // , CountDistinct }
            if (endStep instanceof CountGlobalStep
                    && endStep.getPreviousStep() instanceof DedupGlobalStep) {
                aggFn.setAggregate(FfiAggOpt.CountDistinct); // group().by(..).by(dedup().count())

            } else if (endStep instanceof FoldStep
                    && endStep.getPreviousStep() instanceof DedupGlobalStep) {
                aggFn.setAggregate(FfiAggOpt.ToSet); // group().by(dedup().fold())
            }
            if (admin.getSteps().size() > 1) {
                // generate variables of the aggregate function
                ExprArg exprArg =
                        new ExprArg(admin.getSteps().subList(0, admin.getSteps().size() - 1));
                ExprResult exprRes = getSubTraversalAsExpr(exprArg);
                if (exprRes.isExprPattern()) { // group().by(..).by(select("a").by("name").count())
                    // or group().by(dedup("a").by("name").count())
                    Optional<String> singleExpr = exprRes.getSingleExpr();
                    if (!singleExpr.isPresent()) {
                        throw new OpArgIllegalException(
                                OpArgIllegalException.Cause.INVALID_TYPE,
                                "aggregate value should exist");
                    }
                    aggFn.setVar(getExpressionAsVar(singleExpr.get()));
                } else { // group().by(..).by(out().count())
                    throw new OpArgIllegalException(
                            OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                            "segment apply is unsupported");
                }
            }
            return Collections.singletonList(aggFn);
        }
    },
    WHERE_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            List<InterOpBase> interOpList = new ArrayList<>();

            WherePredicateStep step = (WherePredicateStep) parent;
            Optional<String> startKey = step.getStartKey();
            TraversalRing traversalRing =
                    Utils.getFieldValue(WherePredicateStep.class, step, "traversalRing");

            int stepId = TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            AtomicInteger subId = new AtomicInteger(0);

            String startTag = startKey.isPresent() ? startKey.get() : "";
            String startExpr =
                    getExprWithApplys(startTag, traversalRing.next(), stepId, subId, interOpList);

            P predicate = (P) step.getPredicate().get();
            List<String> selectKeys =
                    Utils.getFieldValue(WherePredicateStep.class, step, "selectKeys");
            traverseAndUpdateP(
                    predicate, selectKeys.iterator(), traversalRing, stepId, subId, interOpList);

            String expr =
                    PredicateExprTransformFactory.HAS_STEP.flatPredicate(startExpr, predicate);
            SelectOp selectOp = new SelectOp();
            selectOp.setPredicate(new OpArg(expr));

            interOpList.add(selectOp);
            return interOpList;
        }

        private void traverseAndUpdateP(
                P predicate,
                Iterator<String> selectKeysIterator,
                TraversalRing traversalRing,
                int stepId,
                AtomicInteger subId,
                List<InterOpBase> applys) {
            if (predicate instanceof ConnectiveP) {
                ((ConnectiveP) predicate)
                        .getPredicates()
                        .forEach(
                                p1 -> {
                                    traverseAndUpdateP(
                                            (P) p1,
                                            selectKeysIterator,
                                            traversalRing,
                                            stepId,
                                            subId,
                                            applys);
                                });
            } else {
                String expr =
                        getExprWithApplys(
                                selectKeysIterator.next(),
                                traversalRing.next(),
                                stepId,
                                subId,
                                applys);
                FfiVariable.ByValue var = getExpressionAsVar(expr);
                predicate.setValue(var);
            }
        }

        private String getExprWithApplys(
                String tag,
                Traversal.Admin whereby,
                int stepId,
                AtomicInteger subId,
                List<InterOpBase> applys) {
            Traversal.Admin tagBy = asSelectTraversal(tag, whereby);
            ExprResult exprRes = getSubTraversalAsExpr(new ExprArg(tagBy));
            if (!exprRes.isExprPattern()) {
                ApplyOp applyOp = new ApplyOp();
                applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                // put select("") in apply
                Traversal copy = GremlinAntlrToJava.getTraversalSupplier().get();
                copy.asAdmin().addStep(new SelectOneStep(copy.asAdmin(), Pop.last, tag));
                // copy steps in by(..) to apply
                whereby.getSteps().forEach(s -> copy.asAdmin().addStep((Step) s));
                applyOp.setSubOpCollection(
                        new OpArg(new InterOpCollectionBuilder(copy).build(applyOp)));
                FfiAlias.ByValue applyAlias =
                        AliasManager.getFfiAlias(
                                new AliasArg(
                                        AliasPrefixType.DEFAULT, stepId, subId.getAndIncrement()));
                applyOp.setAlias(new OpArg(applyAlias));
                applys.add(applyOp);
                return "@" + applyAlias.alias.name;
            } else {
                return exprRes.getSingleExpr().get();
            }
        }

        private Traversal.Admin asSelectTraversal(String selectKey, Traversal.Admin byTraversal) {
            Traversal.Admin traversal =
                    (Traversal.Admin) GremlinAntlrToJava.getTraversalSupplier().get();
            SelectOneStep oneStep = new SelectOneStep(traversal, Pop.last, selectKey);
            oneStep.modulateBy(byTraversal);
            traversal.addStep(oneStep);
            return traversal;
        }
    },
    WHERE_TRAVERSAL_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            Traversal.Admin subTraversal = getWhereSubTraversal(parent.asStep());
            ExprResult exprRes = getSubTraversalAsExpr(new ExprArg(subTraversal));
            if (exprRes.isExprPattern()) {
                String expr = exprRes.getSingleExpr().get();
                SelectOp selectOp = new SelectOp();
                selectOp.setPredicate(new OpArg(expr));
                return Collections.singletonList(selectOp);
            } else { // apply
                ApplyOp applyOp = new ApplyOp();
                FfiJoinKind joinKind = FfiJoinKind.Semi;
                applyOp.setJoinKind(new OpArg(joinKind, Function.identity()));
                applyOp.setSubOpCollection(
                        new OpArg(new InterOpCollectionBuilder(subTraversal).build(applyOp)));
                return Collections.singletonList(applyOp);
            }
        }

        private Traversal.Admin getWhereSubTraversal(Step step) {
            if (step instanceof TraversalFilterStep) {
                return ((TraversalFilterStep) step).getFilterTraversal();
            } else if (step instanceof WhereTraversalStep) {
                WhereTraversalStep whereStep = (WhereTraversalStep) step;
                List<Traversal.Admin> subTraversals = whereStep.getLocalChildren();
                return subTraversals.isEmpty() ? null : subTraversals.get(0);
            } else {
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.INVALID_TYPE,
                        "cannot get where traversal from " + step.getClass());
            }
        }
    },
    NOT_TRAVERSAL_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            Traversal.Admin subTraversal = getNotSubTraversal(parent.asStep());
            ExprResult exprRes = getSubTraversalAsExpr(new ExprArg(subTraversal));
            if (exprRes.isExprPattern()) { // not(select("a").by("name"))
                String notExpr = getNotExpr(exprRes.getSingleExpr().get());
                SelectOp selectOp = new SelectOp();
                selectOp.setPredicate(new OpArg(notExpr));
                return Collections.singletonList(selectOp);
            } else { // apply
                ApplyOp applyOp = new ApplyOp();
                FfiJoinKind joinKind = FfiJoinKind.Anti;
                applyOp.setJoinKind(new OpArg(joinKind));
                applyOp.setSubOpCollection(
                        new OpArg(new InterOpCollectionBuilder(subTraversal).build(applyOp)));
                return Collections.singletonList(applyOp);
            }
        }

        private Traversal.Admin getNotSubTraversal(Step step) {
            if (step instanceof NotStep) {
                NotStep notStep = (NotStep) step;
                List<Traversal.Admin> subTraversals = notStep.getLocalChildren();
                return subTraversals.isEmpty() ? null : subTraversals.get(0);
            } else {
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.INVALID_TYPE,
                        "cannot get where traversal from " + step.getClass());
            }
        }

        private String getNotExpr(String expr) {
            return (expr.contains("&&") || expr.contains("||"))
                    ? String.format("!(%s)", expr)
                    : "!" + expr;
        }
    }
}
