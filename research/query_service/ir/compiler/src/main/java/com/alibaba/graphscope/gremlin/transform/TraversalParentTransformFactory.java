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
import com.alibaba.graphscope.gremlin.transform.alias.AliasManager;
import com.alibaba.graphscope.gremlin.transform.alias.AliasPrefixType;
import com.alibaba.graphscope.gremlin.transform.alias.AliasArg;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum TraversalParentTransformFactory implements TraversalParentTransform {
    // select("a").by("name") -> [ProjectOp("@a.name")]
    // select("a").by(out().count()) -> [ApplyOp(select("a").out().count()).as("a_apply"), ProjectOp("@a_apply")]
    // select("a", "b").by("name").by(out().count()) ->
    // [ApplyOp(select("b").out().count()).as("b_apply"), ProjectOp("@a.name", "@b_apply")]
    PROJECT_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            List<InterOpBase> interOpList = new ArrayList<>();
            Map<String, Traversal.Admin> byTraversals = getProjectTraversals(parent);
            List<Pair<String, FfiAlias.ByValue>> projectExprWithAlias = new ArrayList<>();
            int stepIdx = TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            int subId = 0;
            ExprResult exprRes = getSubTraversalAsExpr((new ExprArg()).addStep(parent.asStep()));
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
                    applyOp.setSubOpCollection(new OpArg<>(copy, (Traversal traversal) ->
                            (new InterOpCollectionBuilder(traversal)).build()
                    ));
                    // column key of apply result
                    FfiAlias.ByValue applyAlias = AliasManager.getFfiAlias(
                            new AliasArg(AliasPrefixType.PROJECT_TAG, k, stepIdx, subId));
                    applyOp.setAlias(new OpArg(applyAlias, Function.identity()));
                    interOpList.add(applyOp);
                    String aliasName = applyAlias.alias.name;
                    expr = "@" + aliasName;
                }
                FfiAlias.ByValue alias = AliasManager.getFfiAlias(
                        new AliasArg(AliasPrefixType.PROJECT_TAG, k, stepIdx, subId));
                projectExprWithAlias.add(Pair.with(expr, alias));
                ++subId;
            }
            // optimize: if there is only one expression, alias with NONE
            if (projectExprWithAlias.size() == 1) {
                Pair single = projectExprWithAlias.get(0);
                projectExprWithAlias.set(0, single.setAt1(ArgUtils.asFfiNoneAlias()));
            }
            ProjectOp op = new ProjectOp();
            op.setExprWithAlias(new OpArg(projectExprWithAlias));
            interOpList.add(op);
            return interOpList;
        }
    },
    // order().by("name"), order().by(values("name")) -> [OrderOp("@.name")]
    // order().by(valueMap("name")) -> can not convert to FfiVariable with valueMap
    // order().by(select("a").by("name")), order().by(select("a").by(values("name"))) -> OrderOp("@a.name")
    // order().by(out().count) -> [ApplyOp(out().count()).as("order_1_apply"), OrderOp("@order_1_apply")]
    // order().by("name").by(out().count) -> [ApplyOp(out().count()).as("order_2_apply"), OrderOp("@.name", "@order_2_apply")]
    ORDER_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            OrderGlobalStep orderStep = (OrderGlobalStep) parent;
            List<InterOpBase> interOpList = new ArrayList<>();
            List<Pair<String, FfiOrderOpt>> exprWithOrderList = new ArrayList<>();
            List<Pair> comparators = orderStep.getComparators();
            int stepIdx = TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            for (int i = 0; i < comparators.size(); ++i) {
                Pair pair = comparators.get(i);
                Traversal.Admin admin = (Traversal.Admin) pair.getValue0();
                FfiOrderOpt orderOpt = getFfiOrderOpt((Order) pair.getValue1());
                ExprResult exprRes = getSubTraversalAsExpr(new ExprArg(admin));
                // i.e. order().by(values("name"))
                if (exprRes.isExprPattern()) {
                    List<String> exprs = exprRes.getExprs();
                    exprs.forEach(k -> {
                        exprWithOrderList.add(Pair.with(k, orderOpt));
                    });
                } else { // use apply, i.e. order().by(out().count())
                    ApplyOp applyOp = new ApplyOp();
                    applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                    applyOp.setSubOpCollection(new OpArg<>(admin, (Traversal traversal) ->
                            (new InterOpCollectionBuilder(traversal)).build()
                    ));
                    FfiAlias.ByValue applyAlias = AliasManager.getFfiAlias(
                            new AliasArg(AliasPrefixType.DEFAULT, stepIdx, i));
                    applyOp.setAlias(new OpArg(applyAlias, Function.identity()));
                    interOpList.add(applyOp);
                    String aliasName = applyAlias.alias.name;
                    exprWithOrderList.add(Pair.with("@" + aliasName, orderOpt));
                }
            }
            OrderOp orderOp = new OrderOp();
            List varWithOrder = exprWithOrderList.stream()
                    .map(k -> {
                        String expr = (String) ((Pair) k).getValue0();
                        FfiVariable.ByValue var = getExpressionAsVar(expr);
                        return ((Pair) k).setAt0(var);
                    }).collect(Collectors.toList());
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
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "invalid order type");
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
            int stepIdx = TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            ExprResult exprRes = getSubTraversalAsExpr(new ExprArg(groupKeyTraversal));
            // i.e. group().by("name")
            if (exprRes.isExprPattern()) {
                groupKeyExprs.addAll(exprRes.getExprs());
            } else { // use apply, i.e. group().by(out().count())
                ApplyOp applyOp = new ApplyOp();
                applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                applyOp.setSubOpCollection(new OpArg<>(groupKeyTraversal, (Traversal traversal) ->
                        (new InterOpCollectionBuilder(traversal)).build()
                ));
                FfiAlias.ByValue applyAlias = AliasManager.getFfiAlias(
                        new AliasArg(AliasPrefixType.GROUP_KEYS, stepIdx));
                applyOp.setAlias(new OpArg(applyAlias, Function.identity()));
                interOpList.add(applyOp);
                String aliasName = applyAlias.alias.name;
                groupKeyExprs.add("@" + aliasName);
            }
            List<Pair<FfiVariable.ByValue, FfiAlias.ByValue>> groupKeyVarWithAlias = new ArrayList<>();
            for (int i = 0; i < groupKeyExprs.size(); ++i) {
                String expr = groupKeyExprs.get(i);
                FfiVariable.ByValue groupKeyVar = getExpressionAsVar(expr);
                FfiAlias.ByValue groupKeyAlias = AliasManager.getFfiAlias(
                        new AliasArg(AliasPrefixType.GROUP_KEYS, stepIdx, i));
                groupKeyVarWithAlias.add(Pair.with(groupKeyVar, groupKeyAlias));
            }
            Step endStep;
            // group().by(values("name").as("a")), "a" is the query given alias of group key
            if (groupKeyTraversal != null
                    && !((endStep = groupKeyTraversal.getEndStep()) instanceof EmptyStep) && !endStep.getLabels().isEmpty()) {
                String queryAlias = (String) endStep.getLabels().iterator().next();
                if (groupKeyVarWithAlias.size() == 1) {
                    Pair newPair = groupKeyVarWithAlias.get(0).setAt1(ArgUtils.asFfiAlias(queryAlias, true));
                    groupKeyVarWithAlias.set(0, newPair);
                } else if (groupKeyVarWithAlias.size() > 1) {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE,
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
                throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "cannot get key traversal from " + step.getClass());
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
                throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "cannot get value traversal from " + step.getClass());
            }
        }

        public List<ArgAggFn> getGroupValueAsAggFn(TraversalParent parent) {
            Traversal.Admin admin = getValueTraversal(parent);
            FfiAggOpt aggOpt;
            int stepIdx = TraversalHelper.stepIndex(parent.asStep(), parent.asStep().getTraversal());
            FfiAlias.ByValue alias = AliasManager.getFfiAlias(
                    new AliasArg(AliasPrefixType.GROUP_VALUES, stepIdx));
            String notice = "supported pattern is [group().by(..).by(count())] or [group().by(..).by(fold())]";
            if (admin == null || admin instanceof IdentityTraversal || admin.getSteps().size() == 2
                    && isMapIdentity(admin.getStartStep()) && admin.getEndStep() instanceof FoldStep) { // group, // group().by(..).by()
                aggOpt = FfiAggOpt.ToList;
            } else if (admin.getSteps().size() == 1) {
                if (admin.getStartStep() instanceof CountGlobalStep) { // group().by(..).by(count())
                    aggOpt = FfiAggOpt.Count;
                } else if (admin.getStartStep() instanceof FoldStep) { // group().by(..).by(fold())
                    aggOpt = FfiAggOpt.ToList;
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, notice);
                }
                // group().by(..).by(count().as("a")), "a" is the query given alias of group value
                Set<String> labels = admin.getStartStep().getLabels();
                if (labels != null && !labels.isEmpty()) {
                    String label = labels.iterator().next();
                    alias = ArgUtils.asFfiAlias(label, true);
                }
            } else {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, notice);
            }
            return Collections.singletonList(new ArgAggFn(aggOpt, alias));
        }

        // TraversalMapStep(identity)
        private boolean isMapIdentity(Step step) {
            if (!(step instanceof TraversalMapStep)) {
                return false;
            }
            TraversalMapStep mapStep = (TraversalMapStep) step;
            Traversal.Admin mapTraversal = mapStep.getLocalChildren().size() > 0 ? (Traversal.Admin) mapStep.getLocalChildren().get(0) : null;
            return mapTraversal != null && mapTraversal instanceof IdentityTraversal;
        }
    },
    WHERE_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            List<InterOpBase> interOpList = new ArrayList<>();

            WherePredicateStep step = (WherePredicateStep) parent;
            Optional<String> startKey = step.getStartKey();
            TraversalRing traversalRing = Utils.getFieldValue(WherePredicateStep.class, step, "traversalRing");

            String startTag = startKey.isPresent() ? startKey.get() : "";
            Traversal.Admin startSelectBy = asSelectTraversal(startTag, traversalRing.next());
            ExprResult startExprRes = getSubTraversalAsExpr(new ExprArg(startSelectBy));
            if (!startExprRes.isExprPattern()) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "where().by(subtask) is unsupported");
            }
            String startBy = startExprRes.getSingleExpr().get();

            P predicate = (P) step.getPredicate().get();
            List<String> selectKeys = Utils.getFieldValue(WherePredicateStep.class, step, "selectKeys");
            traverseAndUpdateP(predicate, selectKeys.iterator(), traversalRing);

            String expr = PredicateExprTransformFactory.HAS_STEP.flatPredicate(startBy, predicate);
            SelectOp selectOp = new SelectOp();
            selectOp.setPredicate(new OpArg(expr));

            interOpList.add(selectOp);
            return interOpList;
        }

        private void traverseAndUpdateP(P predicate, Iterator<String> selectKeysIterator, TraversalRing traversalRing) {
            if (predicate instanceof ConnectiveP) {
                ((ConnectiveP) predicate).getPredicates().forEach(p1 -> {
                    traverseAndUpdateP((P) p1, selectKeysIterator, traversalRing);
                });
            } else {
                Traversal.Admin selectBy = asSelectTraversal(selectKeysIterator.next(), traversalRing.next());
                ExprResult exprRes = getSubTraversalAsExpr(new ExprArg(selectBy));
                if (!exprRes.isExprPattern()) {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "where().by(subtask) is unsupported");
                }
                String tagProperty = exprRes.getSingleExpr().get();
                predicate.setValue(new WherePredicateValue(tagProperty));
            }
        }

        private Traversal.Admin asSelectTraversal(String selectKey, Traversal.Admin byTraversal) {
            Traversal.Admin traversal = (Traversal.Admin) GremlinAntlrToJava.getTraversalSupplier().get();
            SelectOneStep oneStep = new SelectOneStep(traversal, Pop.last, selectKey);
            oneStep.modulateBy(byTraversal);
            return traversal.addStep(oneStep);
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
                applyOp.setSubOpCollection(new OpArg<>(subTraversal, (Traversal traversal) ->
                        (new InterOpCollectionBuilder(traversal)).build()
                ));
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
                throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "cannot get where traversal from " + step.getClass());
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
                applyOp.setSubOpCollection(new OpArg<>(subTraversal, (Traversal traversal) ->
                        (new InterOpCollectionBuilder(traversal)).build()
                ));
                return Collections.singletonList(applyOp);
            }
        }

        private Traversal.Admin getNotSubTraversal(Step step) {
            if (step instanceof NotStep) {
                NotStep notStep = (NotStep) step;
                List<Traversal.Admin> subTraversals = notStep.getLocalChildren();
                return subTraversals.isEmpty() ? null : subTraversals.get(0);
            } else {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "cannot get where traversal from " + step.getClass());
            }
        }

        private String getNotExpr(String expr) {
            return (expr.contains("&&") || expr.contains("||")) ? String.format("!(%s)", expr) : "!" + expr;
        }
    }
}