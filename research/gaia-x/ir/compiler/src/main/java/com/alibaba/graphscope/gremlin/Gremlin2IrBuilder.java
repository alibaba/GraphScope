package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.*;
import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.ScanFusionOp;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class Gremlin2IrBuilder implements IrPlanBuilder<Traversal, Pointer> {
    public static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private static Map<Class<?>, Function<Step, Supplier>> stepTransformMapper;
    public static Gremlin2IrBuilder INSTANCE = new Gremlin2IrBuilder();

    public static Function<HasContainerHolder, String> predicate2Expr = (HasContainerHolder holder1) -> {
        String expr = "";
        for (int i = 0; i < holder1.getHasContainers().size(); ++i) {
            HasContainer container = holder1.getHasContainers().get(i);
            if (container.getPredicate() instanceof ConnectiveP) {
                throw new IllegalArgumentException("nested predicates are unsupported currently");
            }
            if (container.getPredicate().getBiPredicate() != Compare.eq) {
                throw new IllegalArgumentException("the BiPredicate is unsupported currently");
            }
            if (i > 0) {
                expr += "&&";
            }
            Object value = container.getValue();
            if (value instanceof String) {
                expr += String.format("@.%s == \"%s\"", container.getKey(), value);
            } else {
                expr += String.format("@.%s == %s", container.getKey(), value);
            }
        }
        return expr;
    };

    private Gremlin2IrBuilder() {
    }

    static {
        stepTransformMapper = new HashMap<>();
        stepTransformMapper.put(GraphStep.class, (Step t) -> {
            GraphStep step = (GraphStep) t;
            ScanFusionOp op = new ScanFusionOp(OpTransformFactory.SCAN_FUSION_OP);

            op.setScanOpt(new OpArg<>(step, (GraphStep s1) -> {
                if (s1.returnsVertex()) return FfiScanOpt.Vertex;
                else return FfiScanOpt.Edge;
            }));

            if (step instanceof HasContainerHolder) {
                op.setLabels(new OpArg<>((HasContainerHolder) step, (HasContainerHolder holder) -> {
                    List<String> labels = new ArrayList<>();
                    for (HasContainer container : holder.getHasContainers()) {
                        if (container.getKey().equals(T.label.getAccessor())) {
                            Object value = container.getValue();
                            if (value instanceof String) {
                                labels.add((String) value);
                            } else if (value instanceof List
                                    && ((List) value).size() > 0 && ((List) value).get(0) instanceof String) {
                                labels.addAll((List) value);
                            } else {
                                throw new IllegalArgumentException("label should be string of list of string");
                            }
                            holder.removeHasContainer(container);
                        }
                    }
                    return labels.stream().map(k -> irCoreLib.cstrAsConst(k)).collect(Collectors.toList());
                }));
                op.setPredicate(new OpArg<>((HasContainerHolder) step, predicate2Expr));
            }

            if (step.getIds().length > 0) {
                op.setIds(new OpArg<>(step, (GraphStep s1) -> {
                    List ids = Arrays.asList(s1.getIds());
                    return ids.stream().map((id) -> {
                        if (id instanceof Long) {
                            return irCoreLib.int64AsConst((Long) id);
                        } else {
                            throw new IllegalArgumentException("id type should be long, other types are unsupported currently");
                        }
                    }).collect(Collectors.toList());
                }));
            }

            return op;
        });

        stepTransformMapper.put(HasStep.class, (Step t) -> {
            SelectOp op = new SelectOp(OpTransformFactory.SELECT_OP);
            op.setPredicate(new OpArg<>((HasStep) t, predicate2Expr));
            return op;
        });

        stepTransformMapper.put(VertexStep.class, (Step t) -> {
            VertexStep step = (VertexStep) t;
            ExpandOp op = new ExpandOp(OpTransformFactory.EXPAND_OP);

            op.setDirection(
                    new OpArg<>(step, (VertexStep s1) -> {
                        Direction direction = s1.getDirection();
                        switch (direction) {
                            case IN:
                                return FfiDirection.In;
                            case BOTH:
                                return FfiDirection.Both;
                            case OUT:
                                return FfiDirection.Out;
                            default:
                                throw new IllegalArgumentException("direction type is unsupported currently");
                        }
                    }));

            op.setEdgeOpt(
                    new OpArg<>(step, (VertexStep s1) -> {
                        if (s1.returnsEdge()) {
                            return Boolean.valueOf(true);
                        } else {
                            return Boolean.valueOf(false);
                        }
                    })
            );

            op.setLabels(
                    new OpArg<>(step, (VertexStep s1) -> {
                        return Arrays.stream(s1.getEdgeLabels())
                                .map(k -> irCoreLib.cstrAsNameOrId(k)).collect(Collectors.toList());
                    }));
            return op;
        });
    }

    @Override
    public Pointer apply(Traversal traversal) {
        Pointer ptrPlan = irCoreLib.initLogicalPlan();
        IntByReference oprIdx = new IntByReference(0);
        List<Step> steps = traversal.asAdmin().getSteps();
        for (Step step : steps) {
            Function<Step, Supplier> stepTransform = stepTransformMapper.get(step.getClass());
            if (stepTransform == null) {
                throw new UnsupportedOperationException("step " + step.getClass() + " is unsupported currently");
            }
            Supplier<Pointer> irOpSupplier = stepTransform.apply(step);
            appendIrOp(ptrPlan, irOpSupplier, oprIdx.getValue(), oprIdx);
        }
        return ptrPlan;
    }

    private void appendIrOp(Pointer ptrPlan, Supplier<Pointer> irOpSupplier, int parent, IntByReference oprIdx) {
        if (irOpSupplier instanceof ScanFusionOp) {
            if (((ScanFusionOp) irOpSupplier).getIds().isPresent()) {
                irCoreLib.appendIdxscanOperator(ptrPlan, irOpSupplier.get(), parent, oprIdx);
            } else {
                irCoreLib.appendScanOperator(ptrPlan, irOpSupplier.get(), parent, oprIdx);
            }
        } else if (irOpSupplier instanceof SelectOp) {
            irCoreLib.appendSelectOperator(ptrPlan, irOpSupplier.get(), parent, oprIdx);
        } else if (irOpSupplier instanceof ExpandOp) {
            irCoreLib.appendEdgexpdOperator(ptrPlan, irOpSupplier.get(), parent, oprIdx);
        } else {
            throw new UnsupportedOperationException("op " + irOpSupplier.getClass() + " is unsupported currently");
        }
    }
}
