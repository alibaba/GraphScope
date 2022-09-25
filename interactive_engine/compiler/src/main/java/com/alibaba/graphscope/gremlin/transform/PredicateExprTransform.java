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
import com.alibaba.graphscope.gremlin.antlr4.AnyValue;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.tinkerpop.gremlin.process.traversal.*;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;

// transform predicate into expression
public interface PredicateExprTransform extends Function<Step, String> {
    default String flatPredicate(String subject, P predicate) {
        String expr = "";
        if (predicate instanceof ConnectiveP) {
            ConnectiveP connectiveP = (ConnectiveP) predicate;
            String connector = (connectiveP instanceof AndP) ? " && " : " || ";
            List<P> predicates = connectiveP.getPredicates();
            for (int i = 0; i < predicates.size(); ++i) {
                P cur = predicates.get(i);
                if (cur instanceof ConnectiveP) {
                    throw new OpArgIllegalException(
                            OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                            "composition of and & or is unsupported");
                }
                if (i > 0) {
                    expr += connector;
                }
                String flatPredicate = flatPredicate(subject, cur);
                if (i > 0) {
                    expr += "(" + flatPredicate + ")";
                } else {
                    expr += flatPredicate;
                }
            }
        } else {
            Object predicateValue = predicate.getValue();
            BiPredicate biPredicate = predicate.getBiPredicate();
            if (predicateValue instanceof AnyValue) { // has("age") or hasNot("age")
                // @.age
                String propertyExist = subject;
                if (biPredicate == Compare.eq) { // has("age")
                    expr = propertyExist;
                } else if (biPredicate == Compare.neq) { // hasNot("age")
                    expr = "!" + propertyExist;
                } else {
                    throw new OpArgIllegalException(
                            OpArgIllegalException.Cause.INVALID_TYPE,
                            "property type is invalid in checking property existence, "
                                    + biPredicate.toString());
                }
            } else {
                // format as (subject predicate value), i.e "@a.name within [...]"
                Function<Triple, String> defaultFormat =
                        (Triple t) ->
                                String.format(
                                        "%s %s %s",
                                        t.getLeft(),
                                        t.getMiddle(),
                                        getPredicateValue(t.getRight()));
                if (biPredicate == Compare.eq) {
                    expr += getPredicateExpr(subject, "==", predicateValue, defaultFormat);
                } else if (biPredicate == Compare.neq) {
                    expr += getPredicateExpr(subject, "!=", predicateValue, defaultFormat);
                } else if (biPredicate == Compare.lt) {
                    expr += getPredicateExpr(subject, "<", predicateValue, defaultFormat);
                } else if (biPredicate == Compare.lte) {
                    expr += getPredicateExpr(subject, "<=", predicateValue, defaultFormat);
                } else if (biPredicate == Compare.gt) {
                    expr += getPredicateExpr(subject, ">", predicateValue, defaultFormat);
                } else if (biPredicate == Compare.gte) {
                    expr += getPredicateExpr(subject, ">=", predicateValue, defaultFormat);
                } else if (biPredicate == Contains.within) {
                    expr += getPredicateExpr(subject, "within", predicateValue, defaultFormat);
                } else if (biPredicate == Contains.without) {
                    expr += getPredicateExpr(subject, "without", predicateValue, defaultFormat);
                } else if (biPredicate == Text.containing) {
                    expr +=
                            getPredicateExpr(
                                    subject,
                                    "within",
                                    predicateValue,
                                    // reverse subject and value to implement containing by within
                                    // i.e. @a.name contains "name" as substring -> "name" within
                                    // @a.name
                                    (Triple t) ->
                                            String.format(
                                                    "%s %s %s",
                                                    getPredicateValue(t.getRight()),
                                                    t.getMiddle(),
                                                    t.getLeft()));
                } else if (biPredicate == Text.notContaining) {
                    expr +=
                            getPredicateExpr(
                                    subject,
                                    "without",
                                    predicateValue,
                                    (Triple t) ->
                                            String.format(
                                                    "%s %s %s",
                                                    getPredicateValue(t.getRight()),
                                                    t.getMiddle(),
                                                    t.getLeft()));
                } else {
                    throw new OpArgIllegalException(
                            OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                            "predicate type is unsupported");
                }
            }
        }
        return expr;
    }

    default String getPredicateValue(Object value) {
        String predicateValue;
        if (value instanceof String) {
            predicateValue = String.format("\"%s\"", value);
        } else if (value instanceof List) {
            String content = "";
            List values = (List) value;
            for (int i = 0; i < values.size(); ++i) {
                if (i != 0) {
                    content += ", ";
                }
                Object v = values.get(i);
                if (v instanceof List) {
                    throw new OpArgIllegalException(
                            OpArgIllegalException.Cause.UNSUPPORTED_TYPE,
                            "nested list of predicate value is unsupported");
                }
                content += getPredicateValue(v);
            }
            predicateValue = String.format("[%s]", content);
        } else {
            predicateValue = value.toString();
        }
        return predicateValue;
    }

    default String getPredicateExpr(
            String subject, String predicate, Object value, Function<Triple, String> format) {
        return format.apply(Triple.of(subject, predicate, value));
    }
}
