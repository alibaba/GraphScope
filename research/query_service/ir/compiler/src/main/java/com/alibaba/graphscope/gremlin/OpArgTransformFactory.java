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
import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;

import java.util.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OpArgTransformFactory {
    public static String PROJECT_SELF_PREFIX = "project_";

    public static Function<GraphStep, FfiScanOpt> SCAN_OPT = (GraphStep s1) -> {
        if (s1.returnsVertex()) return FfiScanOpt.Entity;
        else return FfiScanOpt.Relation;
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
        List<Pair<String, FfiAlias.ByValue>> projectWithAliasList = new ArrayList<>();
        map.forEach((k, v) -> {
            String expression;
            if (v == null || v instanceof IdentityTraversal) { // select(..)
                expression = "@" + k;
            } else if (v instanceof ValueTraversal) {  // select(..).by('name')
                expression = String.format("@%s.%s", k, ((ValueTraversal) v).getPropertyKey());
            } else if (v.getSteps().size() == 1 && v.getStartStep() instanceof PropertyMapStep) { // select(..).by(valueMap(''))
                String[] mapKeys = ((PropertyMapStep) v.getStartStep()).getPropertyKeys();
                if (mapKeys.length > 0) {
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append("{");
                    for (int i = 0; i < mapKeys.length; ++i) {
                        if (i > 0) {
                            stringBuilder.append(", ");
                        }
                        stringBuilder.append(String.format("@%s.%s", k, mapKeys[i]));
                    }
                    stringBuilder.append("}");
                    expression = stringBuilder.toString();
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "valueMap() is unsupported");
                }
            } else if (v.getSteps().size() == 1 && v.getStartStep() instanceof PropertiesStep) { // values("name")
                String[] mapKeys = ((PropertiesStep) v.getStartStep()).getPropertyKeys();
                if (mapKeys.length == 0) {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "values() is unsupported");
                }
                if (mapKeys.length > 1) {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                            "use valueMap(..) instead if there are multiple project keys");
                }
                expression = String.format("@%s.%s", k, mapKeys[0]);
            } else {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                        "supported pattern is [select(..)] or [select(..).by('name')] or [select(..).by(valueMap(..))]");
            }
            String formatAlias = formatProjectAlias(expression);
            projectWithAliasList.add(Pair.with(expression, ArgUtils.asFfiAlias(formatAlias, false)));
        });
        return projectWithAliasList;
    };

    private static String formatProjectAlias(String expression) {
        // {@.name, @.id} -> {name, id}
        // {@a.name, @a.id} -> a_{name, id}
        if (expression.startsWith("{") && expression.endsWith("}")) {
            String[] exprs = expression.split(",|\\{|\\}|\\s+");
            // default is head
            String tag = "";
            List<String> properties = new ArrayList<>();
            for (int i = 0; i < exprs.length; ++i) {
                if (exprs[i].isEmpty()) continue;
                String alias = formatProjectAlias(exprs[i]);
                String[] tagProperty = alias.split("_");
                if (tagProperty.length == 1) {
                    properties.add(tagProperty[0]);
                } else if (tagProperty.length > 1) {
                    tag = (tag.isEmpty()) ? tagProperty[0] : tag;
                    properties.add(tagProperty[1]);
                } else {
                    throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE,
                            "alias format is invalid, split length is " + tagProperty.length);
                }
            }
            StringBuilder aliasBuilder = new StringBuilder();
            if (!tag.isEmpty()) {
                aliasBuilder.append(tag);
            }
            if (!properties.isEmpty()) {
                StringBuilder propertiesBuilder = new StringBuilder();
                properties.forEach(p -> {
                    boolean isEmpty = (propertiesBuilder.length() == 0);
                    if (!isEmpty) {
                        propertiesBuilder.append(", ");
                    }
                    propertiesBuilder.append(p);
                });
                if (aliasBuilder.length() > 0) {
                    aliasBuilder.append("_");
                }
                aliasBuilder.append("{" + propertiesBuilder.toString() + "}");
            }
            return aliasBuilder.toString();
        } else { // @a.name -> a_name, @.name -> name, @a -> project_a
            String[] splitExpr = expression.split("\\.|\\s+");
            if (splitExpr.length == 0) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE,
                        "expression invalid, split length is 0");
            }
            String tag = splitExpr[0].replace("@", "");
            String property = "";
            if (splitExpr.length > 1) {
                property = splitExpr[1];
            }
            if (property.isEmpty()) {
                return PROJECT_SELF_PREFIX + tag;
            } else if (tag.isEmpty()) {
                return property;
            } else {
                return tag + "_" + property;
            }
        }
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

    // isQueryGiven is set as true by default
    public static Function<String, FfiAlias.ByValue> STEP_TAG_TO_OP_ALIAS = (String tag) ->
            ArgUtils.asFfiAlias(tag, true);

    public static Function<Traversal.Admin, List<Pair<FfiVariable.ByValue, FfiAlias.ByValue>>>
            GROUP_KEYS_FROM_TRAVERSAL = (Traversal.Admin admin) -> {
        FfiVariable.ByValue variable;
        FfiAlias.ByValue alias;
        if (admin == null || admin instanceof IdentityTraversal) { // group()
            variable = ArgUtils.asNoneVar();
            alias = getGroupKeyAlias(variable);
        } else if (admin instanceof ValueTraversal) { // group().by('name')
            String propertyKey = ((ValueTraversal) admin).getPropertyKey();
            variable = ArgUtils.asVarPropertyOnly(ArgUtils.asFfiProperty(propertyKey));
            alias = getGroupKeyAlias(variable);
        } else if (admin.getSteps().size() == 1 && admin.getStartStep() instanceof PropertiesStep) { // values("name")
            PropertiesStep valueStep = (PropertiesStep) admin.getStartStep();
            String[] valueKeys = valueStep.getPropertyKeys();
            if (valueKeys.length == 0) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "values() is unsupported");
            }
            if (valueKeys.length > 1) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                        "use valueMap(..) instead if there are multiple project keys");
            }
            variable = ArgUtils.asVarPropertyOnly(ArgUtils.asFfiProperty(valueKeys[0]));
            alias = getGroupKeyAlias(variable);
            Set<String> queryAliases = valueStep.getLabels();
            if (queryAliases != null && !queryAliases.isEmpty()) {
                String queryLabel = queryAliases.iterator().next();
                alias = ArgUtils.asFfiAlias(queryLabel, true);
            }
        } else {
            throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                    "supported pattern is [group()] or [group().by('name') or [group().by(values('name'))]]");
        }
        return Collections.singletonList(Pair.with(variable, alias));
    };

    // group key is empty list -> count globally
    public static Function<CountGlobalStep, List> GROUP_KEYS_FROM_COUNT = (CountGlobalStep step) -> Collections.emptyList();

    public static Function<Traversal.Admin, List<ArgAggFn>> GROUP_VALUES_FROM_TRAVERSAL = (Traversal.Admin admin) -> {
        List<FfiVariable.ByValue> noneVars = Collections.emptyList();
        FfiAggOpt aggOpt;
        FfiAlias.ByValue alias;
        String notice = "supported pattern is [group().by(..).by(count())] or [group().by(..).by(fold())]";
        if (admin == null) { // group
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
    };

    // keys or keys_a or keys_name or keys_a_name
    private static FfiAlias.ByValue getGroupKeyAlias(FfiVariable.ByValue key) {
        FfiVariable.ByValue noneVar = ArgUtils.asNoneVar();
        FfiNameOrId.ByValue head = ArgUtils.asNoneNameOrId();
        FfiProperty.ByValue noneKey = ArgUtils.asNoneProperty();
        String alias = "";
        if ((!key.equals(noneVar) && !key.tag.equals(head))) {
            alias = key.tag.name;
        }
        String property = "";
        if (!key.equals(noneVar) && !key.property.equals(noneKey)) {
            property = ArgUtils.getPropertyName(key.property);
        }
        String varAlias = getVarAlias(alias, property);
        String ffiAlias = (varAlias.isEmpty()) ? ArgUtils.groupKeys() : ArgUtils.groupKeys() + "_" + varAlias;
        return ArgUtils.asFfiAlias(ffiAlias, false);
    }

    // values
    private static FfiAlias.ByValue getGroupValueAlias(List<FfiVariable.ByValue> vars, FfiAggOpt aggOpt) {
        String alias = ArgUtils.groupValues();
        // todo: add var into alias name
        if (!vars.isEmpty()) {
            throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "aggregate by vars is unsupported");
        }
        return ArgUtils.asFfiAlias(alias, false);
    }

    // empty or a or name or a_name
    private static String getVarAlias(String alias, String property) {
        if (alias.equals("") && property.equals("")) {
            return "";
        } else if (alias.equals("")) {
            return property;
        } else if (property.equals("")) {
            return alias;
        } else {
            return String.format("%s_%s", alias, property);
        }
    }

    public static Function<Map<String, Traversal.Admin>, List<FfiVariable.ByValue>>
            DEDUP_VARS_FROM_TRAVERSALS = (Map<String, Traversal.Admin> tagTraversals) -> {
        if (tagTraversals.isEmpty()) { // only support dedup()
            return Collections.singletonList(ArgUtils.asNoneVar());
        } else {
            throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "supported pattern is [dedup()]");
        }
    };
}
