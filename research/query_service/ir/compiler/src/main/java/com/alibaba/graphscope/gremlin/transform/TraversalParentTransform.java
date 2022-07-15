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
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.ProjectOp;
import com.alibaba.graphscope.common.jna.type.FfiAggOpt;
import com.alibaba.graphscope.common.jna.type.FfiAlias;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import com.alibaba.graphscope.gremlin.transform.alias.AliasArg;
import com.alibaba.graphscope.gremlin.transform.alias.AliasManager;
import com.alibaba.graphscope.gremlin.transform.alias.AliasPrefixType;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Function;

/**
 * judge whether the sub-traversal is the real apply. Especially, judge select('a', 'b').by(..) as a whole,
 * thus group().by(select('a', 'b').by(..)) can get multiple expressions as different group key or consider it as a apply
 **/
public interface TraversalParentTransform extends Function<TraversalParent, List<InterOpBase>> {
    default ExprResult getSubTraversalAsExpr(ExprArg exprArg) {
        int size = exprArg.size();
        // the followings are considered as expressions instead of apply
        if (size <= 1) {
            if (exprArg.isEmpty()) { // by()
                return (new ExprResult(true)).addTagExpr("", "@");
            } else {
                Step step = exprArg.getStartStep();
                if (step instanceof PropertyMapStep) { // valueMap(..)
                    String[] mapKeys = ((PropertyMapStep) step).getPropertyKeys();
                    if (mapKeys.length > 0) {
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("{");
                        for (int i = 0; i < mapKeys.length; ++i) {
                            if (i > 0) {
                                stringBuilder.append(", ");
                            }
                            stringBuilder.append("@." + mapKeys[i]);
                        }
                        stringBuilder.append("}");
                        return (new ExprResult(true)).addTagExpr("", stringBuilder.toString());
                    } else {
                        // valueMap() -> @.~all
                        return (new ExprResult(true)).addTagExpr("", "@." + ArgUtils.PROPERTY_ALL);
                    }
                } else if (step instanceof PropertiesStep) { // values(..)
                    String[] mapKeys = ((PropertiesStep) step).getPropertyKeys();
                    if (mapKeys.length == 0) {
                        throw new OpArgIllegalException(
                                OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                                "values() is unsupported");
                    }
                    if (mapKeys.length > 1) {
                        throw new OpArgIllegalException(
                                OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                                "use valueMap(..) instead if there are multiple project keys");
                    }
                    return (new ExprResult(true)).addTagExpr("", "@." + mapKeys[0]);
                } else if (step instanceof SelectOneStep || step instanceof SelectStep) {
                    // select('a'), select('a').by()
                    // select('a').by('name'/values/valueMap)
                    // select('a', 'b'), select('a', 'b').by()
                    // select('a', 'b').by('name'/values/valueMap).by('name'/values/valueMap)
                    Map<String, Traversal.Admin> selectBys =
                            getProjectTraversals((TraversalParent) step);
                    ExprResult exprRes = new ExprResult();
                    boolean isExprPattern = true;
                    for (Map.Entry<String, Traversal.Admin> entry : selectBys.entrySet()) {
                        String k = entry.getKey();
                        Traversal.Admin v = entry.getValue();
                        Optional<String> byExpr =
                                getSubTraversalAsExpr(new ExprArg(v)).getSingleExpr();
                        if (byExpr.isPresent()) {
                            String expr = byExpr.get().replace("@", "@" + k);
                            exprRes.addTagExpr(k, expr);
                        } else {
                            isExprPattern = false;
                        }
                    }
                    return exprRes.setExprPattern(isExprPattern);
                } else if (step instanceof WhereTraversalStep.WhereStartStep) { // where(as('a'))
                    WhereTraversalStep.WhereStartStep startStep =
                            (WhereTraversalStep.WhereStartStep) step;
                    String selectKey = (String) startStep.getScopeKeys().iterator().next();
                    return (new ExprResult(true)).addTagExpr(selectKey, "@" + selectKey);
                } else if (step instanceof TraversalMapStep) { // select(keys), select(values)
                    ProjectOp mapOp =
                            (ProjectOp) StepTransformFactory.TRAVERSAL_MAP_STEP.apply(step);
                    List<Pair> pairs = (List<Pair>) mapOp.getExprWithAlias().get().applyArg();
                    String mapExpr = (String) pairs.get(0).getValue0();
                    String mapKey = mapExpr.substring(1);
                    return (new ExprResult(true)).addTagExpr(mapKey, mapExpr);
                } else if (step instanceof DedupGlobalStep) {
                    // support the pattern of dedup by variables, i.e. dedup().by("name") or
                    // dedup("a").by("name")
                    ExprResult exprRes = new ExprResult();
                    boolean isExprPattern = true;

                    DedupGlobalStep dedupStep = (DedupGlobalStep) step;
                    List<Traversal.Admin> traversals = dedupStep.getLocalChildren();
                    // get dedupTraversal nested in by() from dedup step
                    Traversal.Admin dedupTraversal =
                            traversals.isEmpty() ? new IdentityTraversal() : traversals.get(0);
                    // check whether the dedupTraversal can be represented as a expression or a
                    // apply
                    // return string if it is a expression, i.e. dedup().by("name") or
                    // dedup("a").by("name")
                    // return null if it is a apply, i.e. dedup().by(out().count())
                    Optional<String> exprOpt =
                            getSubTraversalAsExpr(new ExprArg(dedupTraversal)).getSingleExpr();
                    // get dedup keys from dedup step, i.e dedup("a") -> ["a"], dedup("a", "b") ->
                    // ["a", "b"]
                    Set<String> dedupKeys = dedupStep.getScopeKeys();
                    for (String key : dedupKeys) {
                        if (exprOpt.isPresent()) { // dedup().by("name") or dedup("a").by("name")
                            String expr = exprOpt.get().replace("@", "@" + key);
                            exprRes.addTagExpr(key, expr);
                        } else { // dedup().by(out().count())
                            isExprPattern = false;
                        }
                    }
                    return exprRes.setExprPattern(isExprPattern);
                } else {
                    return new ExprResult(false);
                }
            }
        } else if (size == 2) {
            Step startStep = exprArg.getStartStep();
            Step endStep = exprArg.getEndStep();
            if ((startStep instanceof SelectOneStep || startStep instanceof TraversalMapStep)
                    && (endStep instanceof PropertiesStep || endStep instanceof PropertyMapStep)) {
                Optional<String> propertyExpr =
                        getSubTraversalAsExpr((new ExprArg(Collections.singletonList(endStep))))
                                .getSingleExpr();
                if (!propertyExpr.isPresent()) {
                    return new ExprResult(false);
                }
                String selectKey = null;
                if (startStep
                        instanceof
                        SelectOneStep) { // select('a').values(..), select('a').valueMap(..)
                    selectKey =
                            (String) ((SelectOneStep) startStep).getScopeKeys().iterator().next();
                } else if (startStep
                        instanceof
                        TraversalMapStep) { // select(keys).values(..), select(values).valueMap(..)
                    ProjectOp mapOp =
                            (ProjectOp) StepTransformFactory.TRAVERSAL_MAP_STEP.apply(startStep);
                    List<Pair> pairs = (List<Pair>) mapOp.getExprWithAlias().get().applyArg();
                    String mapExpr = (String) pairs.get(0).getValue0();
                    selectKey = mapExpr.substring(1);
                }
                String expr = propertyExpr.get().replace("@", "@" + selectKey);
                return (new ExprResult(true)).addTagExpr(selectKey, expr);
            } else {
                return new ExprResult(false);
            }
        } else {
            return new ExprResult(false);
        }
    }

    // @ -> as_none_var
    // @a -> as_var_tag_only("a")
    // @.name -> as_var_property_only("name")
    // @a.name -> as_var("a", "name")
    default FfiVariable.ByValue getExpressionAsVar(String expr) {
        // {@a.name} can not be represented as variable
        if (expr.startsWith("{") && expr.endsWith("}")) {
            throw new OpArgIllegalException(
                    OpArgIllegalException.Cause.INVALID_TYPE,
                    "can not convert expression of valueMap to variable");
        }
        String[] splitExpr = expr.split("\\.");
        if (splitExpr.length == 0) {
            throw new OpArgIllegalException(
                    OpArgIllegalException.Cause.INVALID_TYPE, "expr " + expr + " is invalid");
        }
        // remove first "@"
        String tag = (splitExpr[0].length() > 0) ? splitExpr[0].substring(1) : splitExpr[0];
        // property "" indicates none
        String property = (splitExpr.length > 1) ? splitExpr[1] : "";
        return ArgUtils.asFfiVar(tag, property);
    }

    default Map<String, Traversal.Admin> getProjectTraversals(TraversalParent parent) {
        if (parent instanceof SelectOneStep) {
            SelectOneStep step = (SelectOneStep) parent;
            Traversal.Admin selectTraversal = null;
            List<Traversal.Admin> byTraversals = step.getLocalChildren();
            if (!byTraversals.isEmpty()) {
                selectTraversal = byTraversals.get(0);
            }
            String selectKey = (String) step.getScopeKeys().iterator().next();
            Map<String, Traversal.Admin> selectOneByTraversal = new HashMap<>();
            selectOneByTraversal.put(selectKey, selectTraversal);
            return selectOneByTraversal;
        } else if (parent instanceof SelectStep) {
            SelectStep step = (SelectStep) parent;
            return step.getByTraversals();
        } else {
            throw new OpArgIllegalException(
                    OpArgIllegalException.Cause.INVALID_TYPE,
                    "cannot get project traversals from " + parent.getClass());
        }
    }

    // get FfiAggOpt according to the step type
    // and get the alias which will be attached to the aggregated value (alias is generated by
    // AliasManager in an automatic way or is given by the query)
    // and get aggregate variable, set to NONE by default
    default ArgAggFn getAggFn(Step step, int stepIdx) {
        FfiAlias.ByValue alias =
                AliasManager.getFfiAlias(new AliasArg(AliasPrefixType.GROUP_VALUES, stepIdx));
        FfiAggOpt aggOpt;
        if (step instanceof CountGlobalStep) {
            aggOpt = FfiAggOpt.Count;
        } else if (step instanceof FoldStep) {
            aggOpt = FfiAggOpt.ToList;
        } else if (step instanceof SumGlobalStep) {
            aggOpt = FfiAggOpt.Sum;
        } else if (step instanceof MinGlobalStep) {
            aggOpt = FfiAggOpt.Min;
        } else if (step instanceof MaxGlobalStep) {
            aggOpt = FfiAggOpt.Max;
        } else if (step instanceof MeanGlobalStep) {
            aggOpt = FfiAggOpt.Avg;
        } else {
            throw new OpArgIllegalException(
                    OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                    "invalid aggFn " + step.getClass());
        }
        // group().by(..).by(count().as("a")), "a" is the query given alias of group value
        Set<String> labels = step.getLabels();
        if (labels != null && !labels.isEmpty()) {
            String label = labels.iterator().next();
            alias = ArgUtils.asFfiAlias(label, true);
        }
        // set to NONE by default
        return new ArgAggFn(aggOpt, alias);
    }
}
