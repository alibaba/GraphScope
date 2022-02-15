package com.alibaba.graphscope.gremlin.transform;

import com.alibaba.graphscope.common.exception.OpArgIllegalException;
import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.*;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.gremlin.InterOpCollectionBuilder;
import com.alibaba.graphscope.gremlin.plugin.step.PathExpandStep;
import com.alibaba.graphscope.gremlin.plugin.step.ScanFusionStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum StepTransformFactory implements Function<Step, InterOpBase> {
    GRAPH_STEP {
        @Override
        public InterOpBase apply(Step step) {
            GraphStep graphStep = (GraphStep) step;
            ScanFusionOp op = new ScanFusionOp();
            op.setScanOpt(new OpArg<>(graphStep, SCAN_OPT));

            if (graphStep.getIds().length > 0) {
                op.setIds(new OpArg<>(graphStep, CONST_IDS_FROM_STEP));
            }
            return op;
        }
    },
    SCAN_FUSION_STEP {
        @Override
        public InterOpBase apply(Step step) {
            ScanFusionStep scanFusionStep = (ScanFusionStep) step;
            ScanFusionOp op = new ScanFusionOp();

            op.setScanOpt(new OpArg<>(scanFusionStep, SCAN_OPT));

            // set global ids
            if (scanFusionStep.getIds() != null && scanFusionStep.getIds().length > 0) {
                op.setIds(new OpArg(scanFusionStep, CONST_IDS_FROM_STEP));
            }
            // set labels
            if (!scanFusionStep.getGraphLabels().isEmpty()) {
                List<String> labels = scanFusionStep.getGraphLabels();
                op.setLabels(new OpArg(scanFusionStep, LABELS_FROM_STEP));
            }
            // set other containers as predicates
            if (!scanFusionStep.getHasContainers().isEmpty()) {
                op.setPredicate(new OpArg(step, PredicateExprTransformFactory.HAS_STEP));
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
                op.setPredicate(new OpArg(step, PredicateExprTransformFactory.HAS_STEP));
            }
            return op;
        }
    },
    IS_STEP {
        @Override
        public InterOpBase apply(Step step) {
            SelectOp op = new SelectOp();
            op.setPredicate(new OpArg(step, PredicateExprTransformFactory.IS_STEP));
            return op;
        }
    },
    VERTEX_STEP {
        @Override
        public InterOpBase apply(Step step) {
            ExpandOp op = new ExpandOp();
            op.setDirection(new OpArg<>((VertexStep) step, (VertexStep s1) -> {
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
            }));
            op.setEdgeOpt(new OpArg<>((VertexStep) step, (VertexStep s1) -> {
                if (s1.returnsEdge()) {
                    return Boolean.valueOf(true);
                } else {
                    return Boolean.valueOf(false);
                }
            }));
            // add corner judgement
            if (((VertexStep) step).getEdgeLabels().length > 0) {
                op.setLabels(new OpArg<>((VertexStep) step, (VertexStep s1) ->
                        Arrays.stream(s1.getEdgeLabels()).map(k -> ArgUtils.strAsNameId(k)).collect(Collectors.toList())));
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
            op.setLower(new OpArg(Integer.valueOf(lower)));
            op.setUpper(new OpArg(Integer.valueOf(upper)));
            return op;
        }
    },
    VALUE_MAP_STEP {
        @Override
        public InterOpBase apply(Step step) {
            PropertyMapStep valueMapStep = (PropertyMapStep) step;
            ProjectOp op = new ProjectOp();
            String expr = TraversalParentTransformFactory.PROJECT_BY_STEP
                    .getSubTraversalAsExpr("", getProjectTraversal(valueMapStep));
            op.setExprWithAlias(new OpArg<>(expr, (String expr1) -> {
                FfiAlias.ByValue alias = ArgUtils.asNoneAlias();
                return Collections.singletonList(Pair.with(expr1, alias));
            }));
            return op;
        }

        private Traversal.Admin getProjectTraversal(PropertyMapStep step) {
            return (new DefaultTraversal()).addStep(step);
        }
    },
    VALUES_STEP {
        @Override
        public InterOpBase apply(Step step) {
            PropertiesStep valuesStep = (PropertiesStep) step;
            ProjectOp op = new ProjectOp();
            String expr = TraversalParentTransformFactory.PROJECT_BY_STEP
                    .getSubTraversalAsExpr("", getProjectTraversal(valuesStep));
            op.setExprWithAlias(new OpArg<>(expr, (String expr1) -> {
                FfiAlias.ByValue alias = ArgUtils.asNoneAlias();
                return Collections.singletonList(Pair.with(expr1, alias));
            }));
            return op;
        }

        private Traversal.Admin getProjectTraversal(PropertiesStep step) {
            return (new DefaultTraversal()).addStep(step);
        }
    },
    DEDUP_STEP {
        @Override
        public InterOpBase apply(Step step) {
            DedupGlobalStep dedupStep = (DedupGlobalStep) step;
            Map<String, Traversal.Admin> tagTraversals = getDedupTagTraversal(dedupStep);
            DedupOp op = new DedupOp();
            op.setDedupKeys(new OpArg<>(tagTraversals, (Map<String, Traversal.Admin> map) -> {
                if (tagTraversals.isEmpty()) { // only support dedup()
                    return Collections.singletonList(ArgUtils.asNoneVar());
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "supported pattern is [dedup()]");
                }
            }));
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
    COUNT_STEP {
        @Override
        public InterOpBase apply(Step step) {
            CountGlobalStep countStep = (CountGlobalStep) step;
            GroupOp op = new GroupOp();
            op.setGroupByKeys(new OpArg(Collections.emptyList()));
            op.setGroupByValues(new OpArg(getCountAgg(countStep)));
            return op;
        }

        private List<ArgAggFn> getCountAgg(CountGlobalStep step1) {
            FfiAlias.ByValue ffiAlias = ArgUtils.asFfiAlias(ArgUtils.groupValues(), false);
            if (!step1.getLabels().isEmpty()) {
                String label = (String) step1.getLabels().iterator().next();
                ffiAlias = ArgUtils.asFfiAlias(label, true);
                // count().as("a"), as is the alias of group_values
                step1.removeLabel(label);
            }
            ArgAggFn countAgg = new ArgAggFn(FfiAggOpt.Count, ffiAlias);
            return Collections.singletonList(countAgg);
        }
    },
    PATH_EXPAND_STEP {
        @Override
        public InterOpBase apply(Step step) {
            PathExpandOp op = new PathExpandOp((ExpandOp) VERTEX_STEP.apply(step));
            PathExpandStep pathStep = (PathExpandStep) step;
            op.setLower(new OpArg(Integer.valueOf(pathStep.getLower())));
            op.setUpper(new OpArg(Integer.valueOf(pathStep.getUpper())));
            return op;
        }
    },
    EDGE_VERTEX_STEP {
        @Override
        public InterOpBase apply(Step step) {
            EdgeVertexStep vertexStep = (EdgeVertexStep) step;
            GetVOp op = new GetVOp();
            op.setGetVOpt(new OpArg<>(vertexStep, (EdgeVertexStep edgeVertexStep) -> {
                Direction direction = edgeVertexStep.getDirection();
                switch (direction) {
                    case OUT:
                        return FfiVOpt.Start;
                    case IN:
                        return FfiVOpt.End;
                    case BOTH:
                    default:
                        throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, direction + " cannot be converted to FfiVOpt");
                }
            }));
            return op;
        }
    },
    EDGE_OTHER_STEP {
        @Override
        public InterOpBase apply(Step step) {
            EdgeOtherVertexStep otherStep = (EdgeOtherVertexStep) step;
            GetVOp op = new GetVOp();
            op.setGetVOpt(new OpArg<>(otherStep, (EdgeOtherVertexStep otherStep1) ->
                    FfiVOpt.Other
            ));
            return op;
        }
    },
    WHERE_START_STEP {
        @Override
        public InterOpBase apply(Step step) {
            WhereTraversalStep.WhereStartStep startStep = (WhereTraversalStep.WhereStartStep) step;

            String selectKey = (String) startStep.getScopeKeys().iterator().next();
            String expr = TraversalParentTransformFactory.PROJECT_BY_STEP
                    .getSubTraversalAsExpr(selectKey, new IdentityTraversal());

            ProjectOp op = new ProjectOp();
            op.setExprWithAlias(new OpArg<>(expr, (String expr1) -> {
                FfiAlias.ByValue alias = ArgUtils.asNoneAlias();
                return Collections.singletonList(Pair.with(expr1, alias));
            }));
            return op;
        }
    },
    WHERE_END_STEP {
        @Override
        public InterOpBase apply(Step step) {
            WhereTraversalStep.WhereEndStep endStep = (WhereTraversalStep.WhereEndStep) step;
            SelectOp selectOp = new SelectOp();
            selectOp.setPredicate(new OpArg(endStep, PredicateExprTransformFactory.WHERE_END_STEP));
            return selectOp;
        }
    },
    UNION_STEP {
        @Override
        public InterOpBase apply(Step step) {
            UnionOp unionOp = new UnionOp();
            unionOp.setSubOpCollectionList(new OpArg<>((UnionStep) step, (UnionStep unionStep) -> {
                List<Traversal.Admin> subTraversals = unionStep.getGlobalChildren();
                return subTraversals.stream().filter(k -> k != null)
                        .map(k -> (new InterOpCollectionBuilder(k)).build()).collect(Collectors.toList());
            }));
            return unionOp;
        }
    };

    protected Function<GraphStep, FfiScanOpt> SCAN_OPT = (GraphStep s1) -> {
        if (s1.returnsVertex()) return FfiScanOpt.Entity;
        else return FfiScanOpt.Relation;
    };

    protected Function<GraphStep, List<FfiConst.ByValue>> CONST_IDS_FROM_STEP = (GraphStep s1) ->
            Arrays.stream(s1.getIds()).map((id) -> {
                if (id instanceof Integer) {
                    return ArgUtils.intAsConst((Integer) id);
                } else if (id instanceof Long) {
                    return ArgUtils.longAsConst((Long) id);
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "unimplemented yet");
                }
            }).collect(Collectors.toList());

    protected Function<ScanFusionStep, List<FfiNameOrId.ByValue>> LABELS_FROM_STEP = (ScanFusionStep step) -> {
        List<String> labels = step.getGraphLabels();
        return labels.stream().map(k -> ArgUtils.strAsNameId(k)).collect(Collectors.toList());
    };
}
