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
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OpArgTransformFactory {

    public static Function<GraphStep, FfiScanOpt> SCAN_OPT = (GraphStep s1) -> {
        if (s1.returnsVertex()) return FfiScanOpt.Vertex;
        else return FfiScanOpt.Edge;
    };

    public static Function<GraphStep, List<FfiConst.ByValue>> CONST_IDS_FROM_STEP = (GraphStep s1) ->
            Arrays.stream(s1.getIds()).map((id) -> {
                if (id instanceof Integer) {
                    return ArgUtils.intAsConst((Integer) id);
                } else if (id instanceof Long) {
                    return ArgUtils.longAsConst((Long) id);
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "unimplemented yet");
                }
            }).collect(Collectors.toList());

    public static Function<List<HasContainer>, String> EXPR_FROM_CONTAINERS = (List<HasContainer> containers) -> {
        String expr = "";
        for (int i = 0; i < containers.size(); ++i) {
            if (i > 0) {
                expr += "&&";
            }
            HasContainer container = containers.get(i);
            if (container.getPredicate() instanceof ConnectiveP) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "nested predicate");
            }
            String valueExpr = getValueExpr(container.getValue());
            BiPredicate predicate = container.getPredicate().getBiPredicate();
            if (predicate == Compare.eq) {
                expr += String.format("@.%s == %s", container.getKey(), valueExpr);
            } else if (predicate == Compare.neq) {
                expr += String.format("@.%s != %s", container.getKey(), valueExpr);
            } else if (predicate == Compare.lt) {
                expr += String.format("@.%s < %s", container.getKey(), valueExpr);
            } else if (predicate == Compare.lte) {
                expr += String.format("@.%s <= %s", container.getKey(), valueExpr);
            } else if (predicate == Compare.gt) {
                expr += String.format("@.%s > %s", container.getKey(), valueExpr);
            } else if (predicate == Compare.gte) {
                expr += String.format("@.%s >= %s", container.getKey(), valueExpr);
            } else if (predicate == Contains.within) {
                expr += String.format("@.%s within %s", container.getKey(), valueExpr);
            } else if (predicate == Contains.without) {
                expr += String.format("@.%s without %s", container.getKey(), valueExpr);
            } else {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "predicate type is unsupported");
            }
        }
        return expr;
    };

    public static Function<List<HasContainer>, List<FfiNameOrId.ByValue>> LABELS_FROM_CONTAINERS = (List<HasContainer> containers) -> {
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
        return labels.stream().map(k -> ArgUtils.strAsNameId(k)).collect(Collectors.toList());
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

    public static Function<VertexStep, List<FfiNameOrId.ByValue>> EDGE_LABELS_FROM_STEP = (VertexStep s1) ->
            Arrays.stream(s1.getEdgeLabels()).map(k -> ArgUtils.strAsNameId(k)).collect(Collectors.toList());

    public static Function<Map<String, Traversal.Admin>, List<Pair<String, FfiAlias.ByValue>>>
            PROJECT_EXPR_FROM_BY_TRAVERSALS = (Map<String, Traversal.Admin> map) -> {
        // return List<<expr, FfiAlias>>
        List<Pair<String, FfiAlias.ByValue>> exprWithAlias = new ArrayList<>();
        map.forEach((k, v) -> {
            String expr = "@" + k;
            if (v == null || v instanceof IdentityTraversal) { // select(..)
                exprWithAlias.add(makeProjectPair(expr));
            } else if (v instanceof ValueTraversal) {
                expr = String.format("@%s.%s", k, ((ValueTraversal) v).getPropertyKey()); // select(..).by('name')
                exprWithAlias.add(makeProjectPair(expr));
            } else if (v.getSteps().size() == 1 && v.getStartStep() instanceof PropertyMapStep) { // select(..).by(valueMap(''))
                String[] mapKeys = ((PropertyMapStep) v.getStartStep()).getPropertyKeys();
                if (mapKeys.length > 0) {
                    for (int i = 0; i < mapKeys.length; ++i) {
                        String e1 = String.format("@%s.%s", k, mapKeys[i]);
                        exprWithAlias.add(makeProjectPair(e1));
                    }
                } else {
                    exprWithAlias.add(makeProjectPair(expr));
                }
            } else {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                        "supported pattern is [select(..)] or [selecy(..).by('name')] or [select(..).by(valueMap(..))]");
            }
        });
        return exprWithAlias;
    };

    private static Pair<String, FfiAlias.ByValue> makeProjectPair(String expr) {
        return Pair.with(expr, ArgUtils.asFfiAlias(expr, false));
    }

    public static Function<List<Pair<Traversal.Admin, Comparator>>, List<Pair<FfiVariable.ByValue, FfiOrderOpt>>>
            ORDER_VAR_FROM_COMPARATORS = (List<Pair<Traversal.Admin, Comparator>> list) -> {
        List<Pair<FfiVariable.ByValue, FfiOrderOpt>> vars = new ArrayList<>();
        list.forEach(k -> {
            Traversal.Admin admin = k.getValue0();
            FfiOrderOpt orderOpt = getFfiOrderOpt((Order) k.getValue1());
            // order().by('name', order)
            if (admin != null && admin instanceof ValueTraversal) {
                String key = ((ValueTraversal) admin).getPropertyKey();
                FfiProperty.ByValue property = ArgUtils.asFfiProperty(key);
                vars.add(Pair.with(ArgUtils.asVarPropertyOnly(property), orderOpt));
            } else if (admin == null || admin instanceof IdentityTraversal) { // order, order().by(order)
                vars.add(Pair.with(ArgUtils.asNoneVar(), orderOpt));
            } else {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                        "supported pattern is [order()] or [order().by(order) or order().by('name', order)]");
            }
        });
        return vars;
    };

    public static FfiOrderOpt getFfiOrderOpt(Order order) {
        switch (order) {
            case asc:
                return FfiOrderOpt.Asc;
            case desc:
                return FfiOrderOpt.Desc;
            case shuffle:
                return FfiOrderOpt.Desc;
            default:
                throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "invalid order type");
        }
    }

    public static String getValueExpr(Object value) {
        String valueExpr;
        if (value instanceof String) {
            valueExpr = String.format("\"%s\"", value);
        } else if (value instanceof List) {
            String content = "";
            List values = (List) value;
            for (int i = 0; i < values.size(); ++i) {
                if (i != 0) {
                    content += ", ";
                }
                Object v = values.get(i);
                if (v instanceof List) {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                            "nested list of predicate value is unsupported");
                }
                content += getValueExpr(v);
            }
            valueExpr = String.format("[%s]", content);
        } else {
            valueExpr = value.toString();
        }
        return valueExpr;
    }

    // isQueryGiven is set as true by default
    public static Function<String, FfiAlias.ByValue> STEP_TAG_TO_OP_ALIAS = (String tag) ->
            ArgUtils.asFfiAlias(tag, true);
}
