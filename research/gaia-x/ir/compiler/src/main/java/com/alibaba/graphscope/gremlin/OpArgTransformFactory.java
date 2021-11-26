package com.alibaba.graphscope.gremlin;

import com.alibaba.graphscope.common.exception.OpArgIllegalException;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OpArgTransformFactory {
    private static final IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    public static Function<GraphStep, FfiScanOpt> SCAN_OPT = (GraphStep s1) -> {
        if (s1.returnsVertex()) return FfiScanOpt.Vertex;
        else return FfiScanOpt.Edge;
    };

    public static Function<GraphStep, List> CONST_IDS_FROM_STEP = (GraphStep s1) ->
            Arrays.stream(s1.getIds()).map((id) -> {
                if (id instanceof Long) {
                    return irCoreLib.int64AsConst((Long) id);
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "unimplemented yet");
                }
            }).collect(Collectors.toList());

    public static Function<List<HasContainer>, String> EXPR_FROM_CONTAINERS = (List<HasContainer> containers) -> {
        String expr = "";
        for (int i = 0; i < containers.size(); ++i) {
            HasContainer container = containers.get(i);
            if (container.getPredicate() instanceof ConnectiveP) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "nested predicate");
            }
            if (container.getPredicate().getBiPredicate() != Compare.eq) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "not eq");
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

    public static Function<List<HasContainer>, List> LABELS_FROM_CONTAINERS = (List<HasContainer> containers) -> {
        List<String> labels = new ArrayList<>();
        for (HasContainer container : containers) {
            if (container.getKey().equals(T.label.getAccessor())) {
                Object value = container.getValue();
                if (value instanceof String) {
                    labels.add((String) value);
                } else if (value instanceof List
                        && ((List) value).size() > 0 && ((List) value).get(0) instanceof String) {
                    labels.addAll((List) value);
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                            "label should be string or list of string");
                }
            }
        }
        return labels.stream().map(k -> irCoreLib.cstrAsNameOrId(k)).collect(Collectors.toList());
    };

    public static Function<VertexStep, FfiDirection> DIRECTION_FROM_STEP = (VertexStep s1) -> {
        Direction direction = s1.getDirection();
        switch (direction) {
            case IN:
                return FfiDirection.In;
            case BOTH:
                return FfiDirection.Both;
            case OUT:
                return FfiDirection.Out;
            default:
                throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "invalid direction type");
        }
    };

    public static Function<VertexStep, Boolean> IS_EDGE_FROM_STEP = (VertexStep s1) -> {
        if (s1.returnsEdge()) {
            return Boolean.valueOf(true);
        } else {
            return Boolean.valueOf(false);
        }
    };

    public static Function<VertexStep, List> EDGE_LABELS_FROM_STEP = (VertexStep s1) ->
            Arrays.stream(s1.getEdgeLabels()).map(k -> irCoreLib.cstrAsNameOrId(k)).collect(Collectors.toList());
}
