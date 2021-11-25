package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.exception.OpArgIllegalException;
import com.alibaba.graphscope.common.exception.StepUnsupportedException;
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
                    op.setIds(new OpArg(step, OpArgTransformFactory.ID_CONST));
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
                    op.setLabels(new OpArg(step, OpArgTransformFactory.EXTRACT_LABELS));
                    List<HasContainer> allContainers = tinkerGraphStep.getHasContainers();
                    List<HasContainer> predicates = allContainers.stream()
                            .filter(k -> !k.getKey().equals(T.label.getAccessor()))
                            .collect(Collectors.toList());
                    op.setPredicate(new OpArg(predicates, OpArgTransformFactory.PREDICATE_EXPR));
                }

                if (tinkerGraphStep.getIds().length > 0) {
                    op.setIds(new OpArg(step, OpArgTransformFactory.ID_CONST));
                }

                return op;
            }
        },
        HAS_STEP {
            @Override
            public InterOpBase apply(Step step) {
                SelectOp op = new SelectOp();
                op.setPredicate(new OpArg(((HasStep) step).getHasContainers(), OpArgTransformFactory.PREDICATE_EXPR));
                return op;
            }
        },
        VERTEX_STEP {
            @Override
            public InterOpBase apply(Step step) {
                ExpandOp op = new ExpandOp();
                op.setDirection(new OpArg(step, OpArgTransformFactory.DIRECTION));
                op.setEdgeOpt(new OpArg(step, OpArgTransformFactory.IS_EDGE));
                op.setLabels(new OpArg(step, OpArgTransformFactory.EDGE_LABELS));
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

    public IrPlan build() throws OpArgIllegalException, StepUnsupportedException {
        traversal.asAdmin().applyStrategies();
        IrPlan irPlan = new IrPlan();
        List<Step> steps = traversal.asAdmin().getSteps();
        for (Step step : steps) {
            InterOpBase op;
            if (step instanceof GraphStep) {
                op = StepTransformFactory.GRAPH_STEP.apply(step);
            } else if (step instanceof TinkerGraphStep) {
                op = StepTransformFactory.TINKER_GRAPH_STEP.apply(step);
            } else if (step instanceof VertexStep) {
                op = StepTransformFactory.VERTEX_STEP.apply(step);
            } else if (step instanceof HasStep) {
                op = StepTransformFactory.HAS_STEP.apply(step);
            } else if (step instanceof RangeGlobalStep) {
                op = StepTransformFactory.LIMIT_STEP.apply(step);
            } else {
                throw new StepUnsupportedException(step.getClass(), "unimplemented yet");
            }
            if (op != null) {
                irPlan.appendInterOp(op);
            }
        }
        return irPlan;
    }
}
