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
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * judge whether the sub-traversal of {@link TraversalParent} is the real apply subtask, i.e.
 * order().by(select("a")) is considered as simple variables while order().by(out().count()) is apply.
 **/

/**
 * different forms of sub-traversals in gremlin:
 * select("a").by(out().count())
 * where("a").by(out().count())
 * where(out().count())
 * order().by(out().count()
 * group().by(out().count).by(out().count())
 **/

public interface TraversalParentTransform extends Function<TraversalParent, List<InterOpBase>> {
    // the followings are considered as expression instead of apply:
    // select("a").by() -> @a
    // select("a").by("name"), select("a").by(values("name")) -> @a.name
    // select("a").by(valueMap("name")) -> {@a.name}
    // where("a").by() -> @a
    // select("a").by("name"), where("a").by(values("name")) -> @a.name
    // where("a").by(valueMap("name")) -> {@a.name}
    default String getSubTraversalAsExpr(String tag, Traversal.Admin subTraversal) {
        String expr;
        if (subTraversal == null || subTraversal instanceof IdentityTraversal) {
            expr = "@" + tag;
        } else if (subTraversal instanceof ValueTraversal) {
            String property = ((ValueTraversal) subTraversal).getPropertyKey();
            expr = String.format("@%s.%s", tag, property);
        } else if (subTraversal.getSteps().size() == 1 && subTraversal.getStartStep() instanceof PropertiesStep) {
            String[] mapKeys = ((PropertiesStep) subTraversal.getStartStep()).getPropertyKeys();
            if (mapKeys.length == 0) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "values() is unsupported");
            }
            if (mapKeys.length > 1) {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                        "use valueMap(..) instead if there are multiple project keys");
            }
            expr = String.format("@%s.%s", tag, mapKeys[0]);
        } else if (subTraversal.getSteps().size() == 1 && subTraversal.getStartStep() instanceof PropertyMapStep) {
            String[] mapKeys = ((PropertyMapStep) subTraversal.getStartStep()).getPropertyKeys();
            if (mapKeys.length > 0) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("{");
                for (int i = 0; i < mapKeys.length; ++i) {
                    if (i > 0) {
                        stringBuilder.append(", ");
                    }
                    stringBuilder.append(String.format("@%s.%s", tag, mapKeys[i]));
                }
                stringBuilder.append("}");
                expr = stringBuilder.toString();
            } else {
                throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "valueMap() is unsupported");
            }
        } else {
            throw new OpArgIllegalException(OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                    "supported pattern is [by()] or [by('name')] or [by(values('name'))]");
        }
        return expr;
    }

    // judge whether the subTraversal indicates the property, i.e.
    // ..by()
    // ..by("name")
    // ..by(values("name"))
    // ..by(valueMap("name"))
    default boolean isExpressionPatten(String tag, Traversal.Admin subTraversal) {
        return (subTraversal == null || subTraversal instanceof IdentityTraversal)
                || (subTraversal instanceof ValueTraversal)
                || (subTraversal.getSteps().size() == 1 && subTraversal.getStartStep() instanceof PropertiesStep)
                || (subTraversal.getSteps().size() == 1 && subTraversal.getStartStep() instanceof PropertyMapStep);
    }

    // the followings are considered as expression instead of apply:
    // ..by(select("a")) -> @a
    // ..by(select("a").by()) -> @a
    // ..by(select("a").by("name")), by(select("a").by(values("name"))) -> @a.name
    // ..by(select("a").by(valueMap("name"))) -> {@a.name}
    // where(select("a")) -> @a
    // where(select("a").by()) -> @a
    // where(select("a").by("name")), where(select("a").by(values("name"))) -> @a.name
    // where(select("a").by(valueMap("name"))) -> {@a.name}
    // where(__.as("a")...) -> @a
    default String getProjectSubTraversalAsExpr(Traversal.Admin projectSubTraversal) {
        if (projectSubTraversal.getSteps().size() == 1 && projectSubTraversal.getStartStep() instanceof SelectOneStep) {
            SelectOneStep selectOneStep = (SelectOneStep) projectSubTraversal.getStartStep();
            Map<String, Traversal.Admin> selectTraversals = getProjectTraversals(selectOneStep);
            String selectKey = selectTraversals.entrySet().iterator().next().getKey();
            Traversal.Admin selectTraversal = selectTraversals.entrySet().iterator().next().getValue();
            if (isExpressionPatten(selectKey, selectTraversal)) {
                return getSubTraversalAsExpr(selectKey, selectTraversal);
            } else {
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.INVALID_TYPE, "[ " + projectSubTraversal + " ] is not in expression patten");
            }
        } else if (projectSubTraversal.getSteps().size() == 1 && projectSubTraversal.getStartStep() instanceof WhereTraversalStep.WhereStartStep) {
            WhereTraversalStep.WhereStartStep startStep = (WhereTraversalStep.WhereStartStep) projectSubTraversal.getStartStep();
            String selectKey = (String) startStep.getScopeKeys().iterator().next();
            return getSubTraversalAsExpr(selectKey, null);
        } else {
            // todo: group().by(select("a", "b", "c")) ->  group keys are composed of [@a, @b, @c]
            throw new OpArgIllegalException(
                    OpArgIllegalException.Cause.INVALID_TYPE, "[ " + projectSubTraversal + " ] is not in expression patten");
        }
    }

    default boolean isExpressionPatten(Traversal.Admin projectSubTraversal) {
        if (projectSubTraversal.getSteps().size() == 1 && projectSubTraversal.getStartStep() instanceof SelectOneStep) {
            SelectOneStep selectOneStep = (SelectOneStep) projectSubTraversal.getStartStep();
            Map<String, Traversal.Admin> selectTraversals = getProjectTraversals(selectOneStep);
            String selectKey = selectTraversals.entrySet().iterator().next().getKey();
            Traversal.Admin selectTraversal = selectTraversals.entrySet().iterator().next().getValue();
            return isExpressionPatten(selectKey, selectTraversal);
        } else {
            return projectSubTraversal.getSteps().size() == 1
                    && projectSubTraversal.getStartStep() instanceof WhereTraversalStep.WhereStartStep;
        }
    }

    // @ -> as_none_var
    // @a -> as_var_tag_only("a")
    // @.name -> as_var_property_only("name")
    // @a.name -> as_var("a", "name")
    default FfiVariable.ByValue getExpressionAsVar(String expr) {
        // {@a.name} can not be represented as variable
        if (expr.startsWith("{") && expr.endsWith("}")) {
            throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "can not convert expression of valueMap to variable");
        }
        String[] splitExpr = expr.split("\\.");
        if (splitExpr.length == 0) {
            throw new OpArgIllegalException(OpArgIllegalException.Cause.INVALID_TYPE, "expr " + expr + " is invalid");
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
                    OpArgIllegalException.Cause.INVALID_TYPE, "cannot get project traversals from " + parent.getClass());
        }
    }

    // use the step position in the parent traversal to differentiate multiple select("a")
    // i.e. select("a").select("a")
    default String getUniqueAlias(String expression, Step step) {
        String alias = formatExprAsAlias(expression);
        int stepIdx = TraversalHelper.stepIndex(step, step.getTraversal());
        return (alias.isEmpty()) ? String.valueOf(stepIdx) : alias + "_" + stepIdx;
    }

    // {@.name, @.id} -> {name, id}
    // {@a.name, @a.id} -> a_{name, id}
    // @a.name -> a_name
    // @.name -> name
    // @a -> a
    // @ -> ""
    default String formatExprAsAlias(String expression) {
        if (expression.startsWith("{") && expression.endsWith("}")) {
            String[] exprs = expression.split(",|\\{|\\}|\\s+");
            // default is head
            String tag = "";
            List<String> properties = new ArrayList<>();
            for (int i = 0; i < exprs.length; ++i) {
                if (exprs[i].isEmpty()) continue;
                String alias = formatExprAsAlias(exprs[i]);
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
        } else {
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
                return tag;
            } else if (tag.isEmpty()) {
                return property;
            } else {
                return tag + "_" + property;
            }
        }
    }
}
