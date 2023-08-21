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

package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.exception.OpArgIllegalException;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.gremlin.exception.UnsupportedStepException;
import com.alibaba.graphscope.gremlin.plugin.step.*;
import com.alibaba.graphscope.gremlin.plugin.step.GroupCountStep;
import com.alibaba.graphscope.gremlin.plugin.step.GroupStep;
import com.alibaba.graphscope.gremlin.transform.StepTransformFactory;
import com.alibaba.graphscope.gremlin.transform.TraversalParentTransformFactory;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// build IrPlan from gremlin traversal
public class InterOpCollectionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(InterOpCollectionBuilder.class);
    private Traversal traversal;

    public InterOpCollectionBuilder(Traversal traversal) {
        this.traversal = traversal;
    }

    public InterOpCollection build() throws OpArgIllegalException, UnsupportedStepException {
        InterOpCollection opCollection = new InterOpCollection();
        List<Step> steps = traversal.asAdmin().getSteps();
        int indexer = -1;
        boolean selectOneUnfoldOpt = false;
        boolean afterAsOp = false;
        String tagname = "";
        for (Step step : steps) {
            indexer += 1;
            List<InterOpBase> opList = new ArrayList<>();
            // judge by class type instead of instance
            if (Utils.equalClass(step, GraphStep.class)) {
                opList.add(StepTransformFactory.GRAPH_STEP.apply(step));
            } else if (Utils.equalClass(step, ScanFusionStep.class)) {
                opList.add(StepTransformFactory.SCAN_FUSION_STEP.apply(step));
            } else if (Utils.equalClass(step, ExpandFusionStep.class)) {
                opList.add(StepTransformFactory.EXPAND_FUSION_STEP.apply(step));
            } else if (Utils.equalClass(step, HasStep.class)) {
                opList.add(StepTransformFactory.HAS_STEP.apply(step));
            } else if (Utils.equalClass(step, RangeGlobalStep.class)) {
                opList.add(StepTransformFactory.LIMIT_STEP.apply(step));
            } else if (Utils.equalClass(step, CountGlobalStep.class)
                    || Utils.equalClass(step, SumGlobalStep.class)
                    || Utils.equalClass(step, MaxGlobalStep.class)
                    || Utils.equalClass(step, MinGlobalStep.class)
                    || Utils.equalClass(step, FoldStep.class)
                    || Utils.equalClass(step, MeanGlobalStep.class)) {
                opList.add(StepTransformFactory.AGGREGATE_STEP.apply(step));
            } else if (Utils.equalClass(step, PropertiesStep.class)
                    || Utils.equalClass(step, PropertyMapStep.class)
                    || Utils.equalClass(step, LabelStep.class)
                    || Utils.equalClass(step, IdStep.class)
                    || Utils.equalClass(step, ElementMapStep.class)) {
                opList.add(StepTransformFactory.VALUES_STEP.apply(step));
            } else if (Utils.equalClass(step, IsStep.class)) {
                opList.add(StepTransformFactory.IS_STEP.apply(step));
            } else if (Utils.equalClass(step, EdgeVertexStep.class)) {
                opList.add(StepTransformFactory.EDGE_VERTEX_STEP.apply(step));
            } else if (Utils.equalClass(step, EdgeOtherVertexStep.class)) {
                opList.add(StepTransformFactory.EDGE_OTHER_STEP.apply(step));
            } else if (Utils.equalClass(step, PathExpandStep.class)) {
                opList.add(StepTransformFactory.PATH_EXPAND_STEP.apply(step));
            } else if (Utils.equalClass(step, WhereTraversalStep.WhereStartStep.class)) {
                opList.add(StepTransformFactory.WHERE_START_STEP.apply(step));
            } else if (Utils.equalClass(step, WhereTraversalStep.WhereEndStep.class)) {
                opList.add(StepTransformFactory.WHERE_END_STEP.apply(step));
            } else if (Utils.equalClass(step, UnionStep.class)) {
                opList.add(StepTransformFactory.UNION_STEP.apply(step));
            } else if (Utils.equalClass(step, TraversalMapStep.class)) {
                opList.add(StepTransformFactory.TRAVERSAL_MAP_STEP.apply(step));
            } else if (Utils.equalClass(step, SelectOneStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.PROJECT_BY_STEP.apply(
                                (TraversalParent) step));
            } else if (Utils.equalClass(step, SelectStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.PROJECT_BY_STEP.apply(
                                (TraversalParent) step));
            } else if (Utils.equalClass(step, DedupGlobalStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.DEDUP_STEP.apply((TraversalParent) step));
            } else if (Utils.equalClass(step, OrderGlobalStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.ORDER_BY_STEP.apply(
                                (TraversalParent) step));
            } else if (Utils.equalClass(step, GroupStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.GROUP_BY_STEP.apply(
                                (TraversalParent) step));
            } else if (Utils.equalClass(step, GroupCountStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.GROUP_BY_STEP.apply(
                                (TraversalParent) step));
            } else if (Utils.equalClass(step, WherePredicateStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.WHERE_BY_STEP.apply(
                                (TraversalParent) step));
            } else if (Utils.equalClass(step, TraversalFilterStep.class)
                    || Utils.equalClass(step, WhereTraversalStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.WHERE_TRAVERSAL_STEP.apply(
                                (TraversalParent) step));
            } else if (Utils.equalClass(step, NotStep.class)) {
                opList.addAll(
                        TraversalParentTransformFactory.NOT_TRAVERSAL_STEP.apply(
                                (TraversalParent) step));
            } else if (Utils.equalClass(step, MatchStep.class)) {
                opList.add(StepTransformFactory.MATCH_STEP.apply(step));
            } else if (Utils.equalClass(step, ExprStep.class)) {
                opList.add(StepTransformFactory.EXPR_STEP.apply(step));
            } else if (Utils.equalClass(step, SubgraphStep.class)) {
                opList.add(StepTransformFactory.SUBGRAPH_STEP.apply(step));
            } else if (Utils.equalClass(step, IdentityStep.class)) {
                opList.add(StepTransformFactory.IDENTITY_STEP.apply(step));
            } else if (Utils.equalClass(step, ConstantStep.class)) {
                opList.add(StepTransformFactory.CONSTANT_STEP.apply(step));
            } else if (Utils.equalClass(step, UnfoldStep.class)) {
                UnfoldOp unfoldOp = (UnfoldOp) StepTransformFactory.UNFOLD_STEP.apply(step);
                if (selectOneUnfoldOpt) {
                    unfoldOp.setUnfoldTag(new OpArg<>(ArgUtils.asAlias(tagname, true)));
                } else if (afterAsOp) {
                    unfoldOp.setUnfoldTag(new OpArg<>(ArgUtils.asAlias(tagname, true)));
                }
                // System.out.println(unfoldOp.getUnfoldTag().get().getArg());
                opList.add(unfoldOp);
            } else {
                throw new UnsupportedStepException(step.getClass(), "unimplemented yet");
            }
            selectOneUnfoldOpt = false;
            afterAsOp = false;
            if (Utils.equalClass(step, SelectOneStep.class)
                    && indexer < steps.size() - 1
                    && opList.size() == 1
                    && Utils.equalClass(steps.get(indexer + 1), UnfoldStep.class)) {
                ProjectOp projectOp = (ProjectOp) opList.get(0);
                List<Pair<String, FfiAlias.ByValue>> pairList =
                        (List<Pair<String, FfiAlias.ByValue>>)
                                projectOp.getExprWithAlias().get().getArg();
                Pair single = pairList.get(0);
                tagname = (String) single.getValue0();
                tagname = tagname.substring(1);
                // System.out.println(tagname);
                selectOneUnfoldOpt = true;
                continue;
            }

            // System.out.println("come here");
            // System.out.println(step);
            // System.out.println(opList);
            // System.out.println(step.getLabels());

            for (int i = 0; i < opList.size(); ++i) {
                InterOpBase op = opList.get(i);

                // last op
                if (i == opList.size() - 1) {
                    // set alias
                    if (step.getLabels().size() > 1) {
                        logger.error(
                                "multiple aliases of one object is unsupported, take the first and"
                                        + " ignore others");
                    }
                    if (!step.getLabels().isEmpty()) {
                        String label = (String) step.getLabels().iterator().next();
                        op.setAlias(new OpArg(ArgUtils.asAlias(label, true)));

                        // System.out.println("=====================hit");
                        // System.out.println(label);
                        // System.out.println(op.getAlias().get().getArg());

                        afterAsOp = true;
                        tagname = label;
                    } else {
                        afterAsOp = false;
                    }
                }
                opCollection.appendInterOp(op);
            }
        }
        return opCollection;
    }
}
