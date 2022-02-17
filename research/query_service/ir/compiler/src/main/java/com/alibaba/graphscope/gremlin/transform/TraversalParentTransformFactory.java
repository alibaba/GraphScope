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
            List<String> projectExprList = new ArrayList<>();
            int byIdx = 0;
            for (Map.Entry<String, Traversal.Admin> entry : byTraversals.entrySet()) {
                String k = entry.getKey();
                Traversal.Admin v = entry.getValue();
                if (isExpressionPatten(k, v)) {
                    projectExprList.add(getSubTraversalAsExpr(k, v));
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
                    String applyAlias = getApplyAlias(k, byIdx);
                    applyOp.setAlias(new OpArg(ArgUtils.asFfiAlias(applyAlias, false), Function.identity()));
                    interOpList.add(applyOp);
                    projectExprList.add("@" + applyAlias);
                }
                ++byIdx;
            }
            ProjectOp op = new ProjectOp();
            op.setExprWithAlias(new OpArg<>(projectExprList, (List<String> exprList) ->
                    exprList.stream().map(k -> {
                        FfiAlias.ByValue ffiAlias;
                        // optimize: if there is only one expression, alias with NONE
                        if (exprList.size() == 1) {
                            ffiAlias = ArgUtils.asFfiNoneAlias();
                        } else {
                            String exprAsAlias = getUniqueAlias(k, parent.asStep());
                            ffiAlias = ArgUtils.asFfiAlias(exprAsAlias, false);
                        }
                        return Pair.with(k, ffiAlias);
                    }).collect(Collectors.toList())
            ));
            interOpList.add(op);
            return interOpList;
        }

        private String getApplyAlias(String tag, int byIdx) {
            return tag + "_apply_" + byIdx;
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
            for (int i = 0; i < comparators.size(); ++i) {
                Pair pair = comparators.get(i);
                Traversal.Admin admin = (Traversal.Admin) pair.getValue0();
                FfiOrderOpt orderOpt = getFfiOrderOpt((Order) pair.getValue1());
                String orderKeyExpr;
                // i.e. order().by(values("name"))
                if (isExpressionPatten("", admin)) {
                    orderKeyExpr = getSubTraversalAsExpr("", admin);
                    exprWithOrderList.add(Pair.with(orderKeyExpr, orderOpt));
                } else if (isExpressionPatten(admin)) { // i.e. order().by(select("a").by("name"))
                    orderKeyExpr = getProjectSubTraversalAsExpr(admin);
                    exprWithOrderList.add(Pair.with(orderKeyExpr, orderOpt));
                } else { // use apply, i.e. order().by(out().count())
                    ApplyOp applyOp = new ApplyOp();
                    applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                    applyOp.setSubOpCollection(new OpArg<>(admin, (Traversal traversal) ->
                            (new InterOpCollectionBuilder(traversal)).build()
                    ));
                    String applyAlias = getApplyAlias(i);
                    applyOp.setAlias(new OpArg(ArgUtils.asFfiAlias(applyAlias, false), Function.identity()));
                    interOpList.add(applyOp);
                    exprWithOrderList.add(Pair.with("@" + applyAlias, orderOpt));
                }
            }
            OrderOp orderOp = new OrderOp();
            orderOp.setOrderVarWithOrder(new OpArg<>(exprWithOrderList, (List orderList) ->
                    (List) orderList.stream().map(k -> {
                        String expr = (String) ((Pair) k).getValue0();
                        FfiVariable.ByValue var = getExpressionAsVar(expr);
                        return ((Pair) k).setAt0(var);
                    }).collect(Collectors.toList())
            ));
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

        private String getApplyAlias(int byIdx) {
            return "apply_" + byIdx;
        }
    },
    GROUP_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            List<InterOpBase> interOpList = new ArrayList<>();
            // handle group key by
            Traversal.Admin groupKeyTraversal = getKeyTraversal(parent);
            String groupKeyExpr;
            // i.e. group().by("name")
            if (isExpressionPatten("", groupKeyTraversal)) {
                groupKeyExpr = getSubTraversalAsExpr("", groupKeyTraversal);
            } else if (isExpressionPatten(groupKeyTraversal)) { // i.e. group().by(select("a").by("name"))
                groupKeyExpr = getProjectSubTraversalAsExpr(groupKeyTraversal);
            } else { // use apply, i.e. group().by(out().count())
                ApplyOp applyOp = new ApplyOp();
                applyOp.setJoinKind(new OpArg(FfiJoinKind.Inner));
                applyOp.setSubOpCollection(new OpArg<>(groupKeyTraversal, (Traversal traversal) ->
                        (new InterOpCollectionBuilder(traversal)).build()
                ));
                String applyAlias = getApplyAlias();
                applyOp.setAlias(new OpArg(ArgUtils.asFfiAlias(applyAlias, false), Function.identity()));
                interOpList.add(applyOp);
                groupKeyExpr = "@" + applyAlias;
            }
            GroupOp groupOp = new GroupOp();
            groupOp.setGroupByKeys(new OpArg<>(groupKeyExpr, (String keyExpr) -> {
                FfiVariable.ByValue groupKeyVar = getExpressionAsVar(groupKeyExpr);
                String groupKeyAlias = formatExprAsGroupKeyAlias(groupKeyExpr);
                FfiAlias.ByValue ffiAlias = ArgUtils.asFfiAlias(groupKeyAlias, false);
                Step endStep;
                if (groupKeyTraversal != null
                        && !((endStep = groupKeyTraversal.getEndStep()) instanceof EmptyStep) && !endStep.getLabels().isEmpty()) {
                    groupKeyAlias = (String) endStep.getLabels().iterator().next();
                    ffiAlias = ArgUtils.asFfiAlias(groupKeyAlias, true);
                }
                return Collections.singletonList(Pair.with(groupKeyVar, ffiAlias));
            }));
            // handle group value by
            Traversal.Admin valueTraversal = getValueTraversal(parent);
            groupOp.setGroupByValues(new OpArg(getGroupValueAsAggFn(valueTraversal), Function.identity()));
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

        // @ -> keys
        // @a -> keys_a
        // @.name -> keys_name
        // @a.name -> keys_a_name
        // @apply -> keys_apply
        private String formatExprAsGroupKeyAlias(String expr) {
            String exprAsAlias = formatExprAsAlias(expr);
            return exprAsAlias.isEmpty() ? ArgUtils.groupKeys() : ArgUtils.groupKeys() + "_" + exprAsAlias;
        }

        // keys or values
        private String getApplyAlias() {
            return "apply";
        }

        public List<ArgAggFn> getGroupValueAsAggFn(Traversal.Admin admin) {
            List<FfiVariable.ByValue> noneVars = Collections.emptyList();
            FfiAggOpt aggOpt;
            FfiAlias.ByValue alias;
            String notice = "supported pattern is [group().by(..).by(count())] or [group().by(..).by(fold())]";
            if (admin == null || admin instanceof IdentityTraversal || admin.getSteps().size() == 2
                    && isMapIdentity(admin.getStartStep()) && admin.getEndStep() instanceof FoldStep) { // group, // group().by(..).by()
                aggOpt = FfiAggOpt.ToList;
                alias = getGroupValueAlias(noneVars, aggOpt);
            } else if (admin.getSteps().size() == 1) {
                if (admin.getStartStep() instanceof CountGlobalStep) { // group().by(..).by(count())
                    aggOpt = FfiAggOpt.Count;
                    alias = getGroupValueAlias(noneVars, aggOpt);
                } else if (admin.getStartStep() instanceof FoldStep) { // group().by(..).by(fold())
                    aggOpt = FfiAggOpt.ToList;
                    alias = getGroupValueAlias(noneVars, aggOpt);
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, notice);
                }
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

        // values
        private FfiAlias.ByValue getGroupValueAlias(List<FfiVariable.ByValue> vars, FfiAggOpt aggOpt) {
            String alias = ArgUtils.groupValues();
            // todo: add var into alias name
            if (!vars.isEmpty()) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "aggregate by vars is unsupported");
            }
            return ArgUtils.asFfiAlias(alias, false);
        }
    },
    // todo: where("a").by(out().count()), support subtask
    WHERE_BY_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            List<InterOpBase> interOpList = new ArrayList<>();

            WherePredicateStep step = (WherePredicateStep) parent;
            Optional<String> startKey = step.getStartKey();
            TraversalRing traversalRing = Utils.getFieldValue(WherePredicateStep.class, step, "traversalRing");

            String startTag = startKey.isPresent() ? startKey.get() : "";
            String startBy = getSubTraversalAsExpr(startTag, traversalRing.next());

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
                String tagProperty = getSubTraversalAsExpr(selectKeysIterator.next(), traversalRing.next());
                predicate.setValue(new WherePredicateValue(tagProperty));
            }
        }
    },
    WHERE_TRAVERSAL_STEP {
        @Override
        public List<InterOpBase> apply(TraversalParent parent) {
            Traversal.Admin subTraversal = getWhereSubTraversal(parent);
            if (isExpressionPatten("", subTraversal)) { // where(values("name"))
                String expr = getSubTraversalAsExpr("", subTraversal);
                SelectOp selectOp = new SelectOp();
                selectOp.setPredicate(new OpArg(expr));
                return Collections.singletonList(selectOp);
            } else if (isExpressionPatten(subTraversal)) { // where(select("a").by("name"))
                String expr = getProjectSubTraversalAsExpr(subTraversal);
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

        private Traversal.Admin getWhereSubTraversal(TraversalParent step) {
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
            Traversal.Admin subTraversal = getNotSubTraversal(parent);
            if (isExpressionPatten("", subTraversal)) { // not(values("name"))
                String expr = getNotExpr(getSubTraversalAsExpr("", subTraversal));
                SelectOp selectOp = new SelectOp();
                selectOp.setPredicate(new OpArg(expr));
                return Collections.singletonList(selectOp);
            } else if (isExpressionPatten(subTraversal)) { // not(select("a").by("name"))
                String notExpr = getNotExpr(getProjectSubTraversalAsExpr(subTraversal));
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

        private Traversal.Admin getNotSubTraversal(TraversalParent step) {
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