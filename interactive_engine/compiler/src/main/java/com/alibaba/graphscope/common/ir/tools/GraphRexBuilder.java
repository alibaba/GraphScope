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

package com.alibaba.graphscope.common.ir.tools;

import com.alibaba.graphscope.common.ir.rex.RexGraphDynamicParam;
import com.google.common.collect.Lists;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GraphRexBuilder extends RexBuilder {
    public GraphRexBuilder(RelDataTypeFactory typeFactory) {
        super(typeFactory);
    }

    public RexGraphDynamicParam makeGraphDynamicParam(String name, int index) {
        return makeGraphDynamicParam(getTypeFactory().createUnknownType(), name, index);
    }

    public RexGraphDynamicParam makeGraphDynamicParam(
            RelDataType dataType, String name, int index) {
        return new RexGraphDynamicParam(dataType, name, index);
    }

    /**
     * fix bug in calcite-1.32.0, type of {@code Sarg} should be inferred as the least restrictive type of all literals in ranges
     * instead of the first literal, which is the original implementation.
     * @param arg
     * @param ranges
     * @return
     */
    @Override
    public RexNode makeIn(RexNode arg, List<? extends RexNode> ranges) {
        List<RexNode> flatRanges = Lists.newArrayList();
        ranges.forEach(
                k -> {
                    if (k.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
                        flatRanges.addAll(((RexCall) k).getOperands());
                    } else {
                        flatRanges.add(k);
                    }
                });
        Method m1 = getMethod("areAssignable", RexNode.class, List.class);
        if ((boolean) invoke(m1, arg, flatRanges)) {
            Method m2 = getMethod("toSarg", Class.class, List.class, RexUnknownAs.class);
            final Sarg sarg = (Sarg) invoke(m2, Comparable.class, flatRanges, RexUnknownAs.UNKNOWN);
            if (sarg != null) {
                final List<RelDataType> types =
                        flatRanges.stream().map(RexNode::getType).collect(Collectors.toList());
                RelDataType sargType =
                        Objects.requireNonNull(
                                typeFactory.leastRestrictive(types),
                                () -> "Can't find leastRestrictive type for SARG among " + types);
                return makeCall(
                        GraphStdOperatorTable.SEARCH,
                        arg,
                        makeSearchArgumentLiteral(sarg, sargType));
            }
        }
        if (ranges.size() == 1) {
            RexNode range = ranges.get(0);
            switch (range.getKind()) {
                    // right operand is a dynamic parameter ( name in $names ), or a variable ( name
                    // in names )
                case DYNAMIC_PARAM:
                case INPUT_REF:
                    return makeCall(GraphStdOperatorTable.IN, arg, ranges.get(0));
            }
        }
        return RexUtil.composeDisjunction(
                this,
                flatRanges.stream()
                        .map(r -> makeCall(SqlStdOperatorTable.EQUALS, arg, r))
                        .collect(Util.toImmutableList()));
    }

    private Method getMethod(String methodName, Class<?>... parameterTypes) {
        try {
            Method method = RexBuilder.class.getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object invoke(Method method, Object... args) {
        try {
            return method.invoke(null, args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
