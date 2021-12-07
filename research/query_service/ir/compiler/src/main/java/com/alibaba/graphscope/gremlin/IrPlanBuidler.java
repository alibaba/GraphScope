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

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.exception.OpArgIllegalException;
import com.alibaba.graphscope.gremlin.exception.UnsupportedStepException;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect.TinkerGraphStep;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

// build IrPlan from gremlin traversal
public class IrPlanBuidler {
    private Traversal traversal;

    public IrPlanBuidler(Traversal traversal) {
        this.traversal = traversal;
    }

    public enum StepTransformFactory implements Function<Step, InterOpBase> {
        GRAPH_STEP {
            @Override
            public InterOpBase apply(Step step) {
                GraphStep graphStep = (GraphStep) step;
                ScanFusionOp op = new ScanFusionOp();
                op.setScanOpt(new OpArg<>(graphStep, (GraphStep t) -> {
                    if (t.returnsVertex()) return FfiScanOpt.Vertex;
                    else return FfiScanOpt.Edge;
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
                        op.setPredicate(new OpArg(predicates, OpArgTransformFactory.EXPR_FROM_CONTAINERS));
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
                    op.setPredicate(new OpArg(containers, OpArgTransformFactory.EXPR_FROM_CONTAINERS));
                }
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
        }
    }

    public IrPlan build() throws OpArgIllegalException, UnsupportedStepException {
        traversal.asAdmin().applyStrategies();
        IrPlan irPlan = new IrPlan();
        List<Step> steps = traversal.asAdmin().getSteps();
        // judge by class type instead of instance
        for (Step step : steps) {
            InterOpBase op;
            if (equalClass(step, GraphStep.class)) {
                op = StepTransformFactory.GRAPH_STEP.apply(step);
            } else if (equalClass(step, TinkerGraphStep.class)) {
                op = StepTransformFactory.TINKER_GRAPH_STEP.apply(step);
            } else if (equalClass(step, VertexStep.class)) {
                op = StepTransformFactory.VERTEX_STEP.apply(step);
            } else if (equalClass(step, HasStep.class)) {
                op = StepTransformFactory.HAS_STEP.apply(step);
            } else if (equalClass(step, RangeGlobalStep.class)) {
                op = StepTransformFactory.LIMIT_STEP.apply(step);
            } else {
                throw new UnsupportedStepException(step.getClass(), "unimplemented yet");
            }
            if (op != null) {
                irPlan.appendInterOp(op);
            }
        }
        return irPlan;
    }

    private boolean equalClass(Step t1, Class<? extends Step> target) {
        return t1.getClass().equals(target);
    }
}
