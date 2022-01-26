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
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.jna.type.FfiJoinKind;
import com.alibaba.graphscope.gremlin.exception.UnsupportedStepException;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.alibaba.graphscope.gremlin.transform.OpArgTransformFactory;
import com.alibaba.graphscope.gremlin.transform.PredicateExprTransformFactory;
import com.alibaba.graphscope.gremlin.transform.ProjectTraversalTransformFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect.TinkerGraphStep;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

// build IrPlan from gremlin traversal
public class InterOpCollectionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(InterOpCollectionBuilder.class);
    private Traversal traversal;

    public InterOpCollectionBuilder(Traversal traversal) {
        this.traversal = traversal;
    }

    public enum StepTransformFactory implements Function<Step, InterOpBase> {
        GRAPH_STEP {
            @Override
            public InterOpBase apply(Step step) {
                GraphStep graphStep = (GraphStep) step;
                ScanFusionOp op = new ScanFusionOp();
                op.setScanOpt(new OpArg<>(graphStep, (GraphStep t) -> {
                    if (t.returnsVertex()) return FfiScanOpt.Entity;
                    else return FfiScanOpt.Relation;
                }));

                if (graphStep.getIds().length > 0) {
                    op.setIds(new OpArg(step, OpArgTransformFactory.CONST_IDS_FROM_STEP));
                }
                return op;
            }
        },
        TINKER_GRAPH_STEP {
            @Override
            public InterOpBase apply(Step step) {
                TinkerGraphStep tinkerGraphStep = (TinkerGraphStep) step;
                ScanFusionOp op = new ScanFusionOp();

                op.setScanOpt(new OpArg(step, OpArgTransformFactory.SCAN_OPT));

                if (!tinkerGraphStep.getHasContainers().isEmpty()) {
                    List<HasContainer> allContainers = tinkerGraphStep.getHasContainers();
                    List<HasContainer> labels = allContainers.stream()
                            .filter(k -> k.getKey().equals(T.label.getAccessor()))
                            .collect(Collectors.toList());
                    // add corner judgement
                    if (!labels.isEmpty()) {
                        op.setLabels(new OpArg(labels, OpArgTransformFactory.LABELS_FROM_CONTAINERS));
                    }
                    List<HasContainer> predicates = new ArrayList<>(allContainers);
                    predicates.removeAll(labels);
                    // add corner judgement
                    if (!predicates.isEmpty()) {
                        op.setPredicate(new OpArg(predicates, PredicateExprTransformFactory.EXPR_FROM_CONTAINERS));
                    }
                }

                if (tinkerGraphStep.getIds().length > 0) {
                    op.setIds(new OpArg(step, OpArgTransformFactory.CONST_IDS_FROM_STEP));
                }

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
                    op.setPredicate(new OpArg(containers, PredicateExprTransformFactory.EXPR_FROM_CONTAINERS));
                }
                return op;
            }
        },
        IS_STEP {
            @Override
            public InterOpBase apply(Step step) {
                SelectOp op = new SelectOp();
                op.setPredicate(new OpArg(step, PredicateExprTransformFactory.EXPR_FROM_IS_STEP));
                return op;
            }
        },
        VERTEX_STEP {
            @Override
            public InterOpBase apply(Step step) {
                ExpandOp op = new ExpandOp();
                op.setDirection(new OpArg(step, OpArgTransformFactory.DIRECTION_FROM_STEP));
                op.setEdgeOpt(new OpArg(step, OpArgTransformFactory.IS_EDGE_FROM_STEP));
                // add corner judgement
                if (((VertexStep) step).getEdgeLabels().length > 0) {
                    op.setLabels(new OpArg(step, OpArgTransformFactory.EDGE_LABELS_FROM_STEP));
                }
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
                op.setLower(new OpArg(Integer.valueOf(lower), Function.identity()));
                op.setUpper(new OpArg(Integer.valueOf(upper), Function.identity()));
                return op;
            }
        },
        SELECT_BY_STEP {
            @Override
            public InterOpBase apply(Step step) {
                SelectStep selectStep = (SelectStep) step;
                Map<String, Traversal.Admin> byTraversals = getProjectTraversals(selectStep);
                ProjectOp op = new ProjectOp();
                if (!byTraversals.isEmpty()) {
                    op.setExprWithAlias(new OpArg(byTraversals, OpArgTransformFactory.PROJECT_EXPR_FROM_BY_TRAVERSALS));
                }
                return op;
            }

            private Map<String, Traversal.Admin> getProjectTraversals(SelectStep step) {
                return step.getByTraversals();
            }
        },
        ORDER_BY_STEP {
            @Override
            public InterOpBase apply(Step step) {
                OrderGlobalStep orderStep = (OrderGlobalStep) step;
                OrderOp op = new OrderOp();
                if (!orderStep.getComparators().isEmpty()) {
                    op.setOrderVarWithOrder(new OpArg(orderStep.getComparators(), OpArgTransformFactory.ORDER_VAR_FROM_COMPARATORS));
                }
                return op;
            }
        },
        VALUE_MAP_STEP {
            @Override
            public InterOpBase apply(Step step) {
                PropertyMapStep valueMapStep = (PropertyMapStep) step;
                ProjectOp op = new ProjectOp();
                Map<String, Traversal.Admin> valueMap = getProjectTraversals(valueMapStep);
                op.setExprWithAlias(new OpArg(valueMap, OpArgTransformFactory.PROJECT_EXPR_FROM_BY_TRAVERSALS));
                return op;
            }

            private Map<String, Traversal.Admin> getProjectTraversals(PropertyMapStep step) {
                Traversal.Admin valueTraversal = (new DefaultTraversal()).addStep(step);
                return ImmutableMap.of("", valueTraversal);
            }
        },
        VALUES_STEP {
            @Override
            public InterOpBase apply(Step step) {
                PropertiesStep valuesStep = (PropertiesStep) step;
                ProjectOp op = new ProjectOp();
                Map<String, Traversal.Admin> valueMap = getProjectTraversals(valuesStep);
                op.setExprWithAlias(new OpArg(valueMap, OpArgTransformFactory.PROJECT_EXPR_FROM_BY_TRAVERSALS));
                return op;
            }

            private Map<String, Traversal.Admin> getProjectTraversals(PropertiesStep step) {
                Traversal.Admin valueTraversal = (new DefaultTraversal()).addStep(step);
                return ImmutableMap.of("", valueTraversal);
            }
        },
        GROUP_STEP {
            @Override
            public InterOpBase apply(Step step) {
                GroupStep groupStep = (GroupStep) step;
                Traversal.Admin key = getKeyTraversal(groupStep);
                GroupOp op = new GroupOp();
                op.setGroupByKeys(new OpArg(key, OpArgTransformFactory.GROUP_KEYS_FROM_TRAVERSAL));
                Traversal.Admin value = getValueTraversal(groupStep);
                op.setGroupByValues(new OpArg(value, OpArgTransformFactory.GROUP_VALUES_FROM_TRAVERSAL));
                return op;
            }

            private Traversal.Admin getKeyTraversal(GroupStep step) {
                return step.getKeyTraversal();
            }

            private Traversal.Admin getValueTraversal(GroupStep step) {
                return step.getValueTraversal();
            }
        },
        GROUP_COUNT_STEP {
            @Override
            public InterOpBase apply(Step step) {
                GroupCountStep groupCountStep = (GroupCountStep) step;
                Traversal.Admin key = getKeyTraversal(groupCountStep);
                GroupOp op = new GroupOp();
                op.setGroupByKeys(new OpArg(key, OpArgTransformFactory.GROUP_KEYS_FROM_TRAVERSAL));
                Traversal.Admin value = getValueTraversal(groupCountStep);
                op.setGroupByValues(new OpArg(value, OpArgTransformFactory.GROUP_VALUES_FROM_TRAVERSAL));
                return op;
            }

            private Traversal.Admin getKeyTraversal(GroupCountStep step) {
                List<Traversal.Admin> keyTraversals = step.getLocalChildren();
                Traversal.Admin keyTraversal = (keyTraversals.isEmpty()) ? null : keyTraversals.get(0);
                return keyTraversal;
            }

            private Traversal.Admin getValueTraversal(GroupCountStep step) {
                Traversal.Admin countTraversal = new DefaultTraversal();
                countTraversal.addStep(new CountGlobalStep(countTraversal));
                return countTraversal;
            }
        },
        DEDUP_STEP {
            @Override
            public InterOpBase apply(Step step) {
                DedupGlobalStep dedupStep = (DedupGlobalStep) step;
                DedupOp op = new DedupOp();
                Map<String, Traversal.Admin> tagTraversals = getDedupTagTraversal(dedupStep);
                op.setDedupKeys(new OpArg(tagTraversals, OpArgTransformFactory.DEDUP_VARS_FROM_TRAVERSALS));
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
                dedupTags.forEach(k -> {
                    Traversal.Admin dedupTraversal = dedupTraversals.isEmpty() ? null : dedupTraversals.get(0);
                    tagTraversals.put(k, dedupTraversal);
                });
                tagTraversals.entrySet().removeIf(e -> e.getKey().equals("") && e.getValue() == null);
                return tagTraversals;
            }
        },
        SELECT_ONE_BY_STEP {
            @Override
            public InterOpBase apply(Step step) {
                Map<String, Traversal.Admin> byTraversals = getProjectTraversals((SelectOneStep) step);
                ProjectOp op = new ProjectOp();
                if (!byTraversals.isEmpty()) {
                    op.setExprWithAlias(new OpArg(byTraversals, OpArgTransformFactory.PROJECT_EXPR_FROM_BY_TRAVERSALS));
                }
                return op;
            }

            private Map<String, Traversal.Admin> getProjectTraversals(SelectOneStep step) {
                Traversal.Admin selectTraversal = null;
                List<Traversal.Admin> byTraversals = step.getLocalChildren();
                if (!byTraversals.isEmpty()) {
                    selectTraversal = byTraversals.get(0);
                }
                String selectKey = (String) step.getScopeKeys().iterator().next();
                Map<String, Traversal.Admin> selectOneByTraversal = new HashMap<>();
                selectOneByTraversal.put(selectKey, selectTraversal);
                return selectOneByTraversal;
            }
        },
        COUNT_STEP {
            @Override
            public InterOpBase apply(Step step) {
                CountGlobalStep countStep = (CountGlobalStep) step;
                GroupOp op = new GroupOp();
                op.setGroupByKeys(new OpArg(countStep, OpArgTransformFactory.GROUP_KEYS_FROM_COUNT));
                Traversal.Admin count = getValueTraversal(countStep);
                op.setGroupByValues(new OpArg(count, OpArgTransformFactory.GROUP_VALUES_FROM_TRAVERSAL));
                return op;
            }

            private Traversal.Admin getValueTraversal(CountGlobalStep step) {
                Traversal.Admin countTraversal = new DefaultTraversal();
                countTraversal.addStep(step);
                return countTraversal;
            }
        },
        WHERE_PREDICATE_STEP {
            @Override
            public InterOpBase apply(Step step) {
                WherePredicateStep whereStep = (WherePredicateStep) step;
                SelectOp op = new SelectOp();
                op.setPredicate(new OpArg(whereStep, PredicateExprTransformFactory.EXPR_FROM_WHERE_PREDICATE));
                return op;
            }
        },
        EDGE_VERTEX_STEP {
            @Override
            public InterOpBase apply(Step step) {
                EdgeVertexStep vertexStep = (EdgeVertexStep) step;
                GetVOp op = new GetVOp();
                op.setGetVOpt(new OpArg(vertexStep, OpArgTransformFactory.GETV_OPT_FROM_STEP));
                return op;
            }
        },
        EDGE_OTHER_STEP {
            @Override
            public InterOpBase apply(Step step) {
                EdgeOtherVertexStep otherStep = (EdgeOtherVertexStep) step;
                GetVOp op = new GetVOp();
                op.setGetVOpt(new OpArg(otherStep, OpArgTransformFactory.GETV_OPT_FROM_STEP));
                return op;
            }
        },
        WHERE_TRAVERSAL_STEP {
            @Override
            public InterOpBase apply(Step step) {
                Traversal.Admin subTraversal = getWhereSubTraversal(step);
                boolean isPropertyPattern = ProjectTraversalTransformFactory.isPropertyPattern(subTraversal);
                boolean isTagPropertyPattern = ProjectTraversalTransformFactory.isTagPropertyPattern(subTraversal);
                if (isPropertyPattern) {
                    String expr = ProjectTraversalTransformFactory.getTagProjectTraversalAsExpr("", subTraversal);
                    SelectOp selectOp = new SelectOp();
                    selectOp.setPredicate(new OpArg(expr, Function.identity()));
                    return selectOp;
                } else if (isTagPropertyPattern) {
                    Pair<String, Traversal.Admin> pair = ProjectTraversalTransformFactory.getProjectTraversalAsTagProperty(subTraversal);
                    String expr = ProjectTraversalTransformFactory.getTagProjectTraversalAsExpr(pair.getValue0(), pair.getValue1());
                    SelectOp selectOp = new SelectOp();
                    selectOp.setPredicate(new OpArg(expr, Function.identity()));
                    return selectOp;
                } else { // subtask
                    ApplyOp applyOp = new ApplyOp();
                    applyOp.setJoinKind(new OpArg(FfiJoinKind.Semi, Function.identity()));
                    applyOp.setSubOpCollection(new OpArg(subTraversal, OpArgTransformFactory.INTER_OPS_FROM_SUB_TRAVERSAL));
                    return applyOp;
                }
            }

            private Traversal.Admin getWhereSubTraversal(Step step) {
                if (step instanceof TraversalFilterStep) {
                    return ((TraversalFilterStep) step).getFilterTraversal();
                } else if (step instanceof WhereTraversalStep) {
                    WhereTraversalStep whereStep = (WhereTraversalStep) step;
                    List<Traversal.Admin> subTraversals = whereStep.getLocalChildren();
                    Traversal.Admin subTraversal = subTraversals.isEmpty() ? null : subTraversals.get(0);
                    return subTraversal;
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "cannot get where traversal from " + step.getClass());
                }
            }
        },
        WHERE_START_STEP {
            @Override
            public InterOpBase apply(Step step) {
                WhereTraversalStep.WhereStartStep startStep = (WhereTraversalStep.WhereStartStep) step;
                Map<String, Traversal.Admin> projectTraversal = getProjectTraversals(startStep);

                ProjectOp op = new ProjectOp();
                op.setExprWithAlias(new OpArg(projectTraversal, OpArgTransformFactory.PROJECT_EXPR_FROM_BY_TRAVERSALS));
                return op;
            }

            private Map<String, Traversal.Admin> getProjectTraversals(WhereTraversalStep.WhereStartStep startStep) {
                String selectKey = (String) startStep.getScopeKeys().iterator().next();
                return ImmutableMap.of(selectKey, new IdentityTraversal());
            }
        },
        WHERE_END_STEP {
            @Override
            public InterOpBase apply(Step step) {
                WhereTraversalStep.WhereEndStep endStep = (WhereTraversalStep.WhereEndStep) step;
                SelectOp selectOp = new SelectOp();
                selectOp.setPredicate(new OpArg(endStep, PredicateExprTransformFactory.EXPR_FROM_WHERE_END));
                return selectOp;
            }
        }
    }

    public InterOpCollection build() throws OpArgIllegalException, UnsupportedStepException {
        InterOpCollection opCollection = new InterOpCollection();
        List<Step> steps = traversal.asAdmin().getSteps();
        // judge by class type instead of instance
        for (Step step : steps) {
            InterOpBase op;
            if (Utils.equalClass(step, GraphStep.class)) {
                op = StepTransformFactory.GRAPH_STEP.apply(step);
            } else if (Utils.equalClass(step, TinkerGraphStep.class)) {
                op = StepTransformFactory.TINKER_GRAPH_STEP.apply(step);
            } else if (Utils.equalClass(step, VertexStep.class)) {
                op = StepTransformFactory.VERTEX_STEP.apply(step);
            } else if (Utils.equalClass(step, HasStep.class)) {
                op = StepTransformFactory.HAS_STEP.apply(step);
            } else if (Utils.equalClass(step, RangeGlobalStep.class)) {
                op = StepTransformFactory.LIMIT_STEP.apply(step);
            } else if (Utils.equalClass(step, SelectStep.class)) {
                op = StepTransformFactory.SELECT_BY_STEP.apply(step);
            } else if (Utils.equalClass(step, OrderGlobalStep.class)) {
                op = StepTransformFactory.ORDER_BY_STEP.apply(step);
            } else if (Utils.equalClass(step, PropertyMapStep.class)) {
                op = StepTransformFactory.VALUE_MAP_STEP.apply(step);
            } else if (Utils.equalClass(step, GroupStep.class)) {
                op = StepTransformFactory.GROUP_STEP.apply(step);
            } else if (Utils.equalClass(step, GroupCountStep.class)) {
                op = StepTransformFactory.GROUP_COUNT_STEP.apply(step);
            } else if (Utils.equalClass(step, DedupGlobalStep.class)) {
                op = StepTransformFactory.DEDUP_STEP.apply(step);
            } else if (Utils.equalClass(step, SelectOneStep.class)) {
                op = StepTransformFactory.SELECT_ONE_BY_STEP.apply(step);
            } else if (Utils.equalClass(step, CountGlobalStep.class)) {
                op = StepTransformFactory.COUNT_STEP.apply(step);
            } else if (Utils.equalClass(step, PropertiesStep.class)) {
                op = StepTransformFactory.VALUES_STEP.apply(step);
            } else if (Utils.equalClass(step, IsStep.class)) {
                op = StepTransformFactory.IS_STEP.apply(step);
            } else if (Utils.equalClass(step, EdgeVertexStep.class)) {
                op = StepTransformFactory.EDGE_VERTEX_STEP.apply(step);
            } else if (Utils.equalClass(step, EdgeOtherVertexStep.class)) {
                op = StepTransformFactory.EDGE_OTHER_STEP.apply(step);
            } else if (Utils.equalClass(step, WherePredicateStep.class)) {
                op = StepTransformFactory.WHERE_PREDICATE_STEP.apply(step);
            } else if (Utils.equalClass(step, TraversalFilterStep.class) || Utils.equalClass(step, WhereTraversalStep.class)) {
                op = StepTransformFactory.WHERE_TRAVERSAL_STEP.apply(step);
            } else if (Utils.equalClass(step, WhereTraversalStep.WhereStartStep.class)) {
                op = StepTransformFactory.WHERE_START_STEP.apply(step);
            } else if (Utils.equalClass(step, WhereTraversalStep.WhereEndStep.class)) {
                op = StepTransformFactory.WHERE_END_STEP.apply(step);
            } else {
                throw new UnsupportedStepException(step.getClass(), "unimplemented yet");
            }
            if (op != null) {
                // set alias
                if (step.getLabels().size() > 1) {
                    logger.error("multiple aliases of one object is unsupported, take the first and ignore others");
                }
                if (!step.getLabels().isEmpty()) {
                    op.setAlias(new OpArg(step.getLabels().iterator().next(), OpArgTransformFactory.STEP_TAG_TO_OP_ALIAS));
                }
                opCollection.appendInterOp(op);
            }
        }
        return opCollection;
    }
}
