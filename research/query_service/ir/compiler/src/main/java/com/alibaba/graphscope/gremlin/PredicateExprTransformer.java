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
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;

// transform predicate into expression
public interface PredicateExprTransformer extends Function {
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
                            OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "composition of and & or is unsupported");
                }
                if (i > 0) {
                    expr += connector;
                }
                expr += flatPredicate(subject, cur);
            }
        } else {
            Object predicateValue = predicate.getValue();
            String valueExpr = getValueExpr(predicateValue);
            BiPredicate biPredicate = predicate.getBiPredicate();
            if (biPredicate == Compare.eq) {
                expr += String.format("%s == %s", subject, valueExpr);
            } else if (biPredicate == Compare.neq) {
                expr += String.format("%s != %s", subject, valueExpr);
            } else if (biPredicate == Compare.lt) {
                expr += String.format("%s < %s", subject, valueExpr);
            } else if (biPredicate == Compare.lte) {
                expr += String.format("%s <= %s", subject, valueExpr);
            } else if (biPredicate == Compare.gt) {
                expr += String.format("%s > %s", subject, valueExpr);
            } else if (biPredicate == Compare.gte) {
                expr += String.format("%s >= %s", subject, valueExpr);
            } else if (biPredicate == Contains.within) {
                expr += String.format("%s within %s", subject, valueExpr);
            } else if (biPredicate == Contains.without) {
                expr += String.format("%s without %s", subject, valueExpr);
            } else {
                throw new OpArgIllegalException(
                        OpArgIllegalException.Cause.UNSUPPORTED_TYPE, "predicate type is unsupported");
            }
        }
        return expr;
    }

    default String getValueExpr(Object value) {
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
}
