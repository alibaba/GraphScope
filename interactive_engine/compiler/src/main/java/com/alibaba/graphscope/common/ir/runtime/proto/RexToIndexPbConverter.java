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

package com.alibaba.graphscope.common.ir.runtime.proto;

import com.alibaba.graphscope.common.ir.rex.RexGraphVariable;
import com.alibaba.graphscope.gaia.proto.GraphAlgebra;
import com.alibaba.graphscope.gaia.proto.OuterExpression;
import com.google.common.base.Preconditions;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

public class RexToIndexPbConverter extends RexVisitorImpl<GraphAlgebra.IndexPredicate> {
    private final boolean isColumnId;
    private final RexBuilder rexBuilder;

    public RexToIndexPbConverter(boolean deep, boolean isColumnId, RexBuilder rexBuilder) {
        super(deep);
        this.isColumnId = isColumnId;
        this.rexBuilder = rexBuilder;
    }

    @Override
    public GraphAlgebra.IndexPredicate visitCall(RexCall call) {
        GraphAlgebra.IndexPredicate.Builder indexBuilder = GraphAlgebra.IndexPredicate.newBuilder();
        for (RexNode disjunction : RelOptUtil.disjunctions(call)) {
            if (disjunction instanceof RexCall) {
                indexBuilder.addOrPredicates(
                        GraphAlgebra.IndexPredicate.AndPredicate.newBuilder()
                                .addPredicates(protoTriplet((RexCall) disjunction))
                                .build());
            } else {
                throw new IllegalArgumentException(
                        "invalid unique key filter pattern=" + disjunction);
            }
        }
        return indexBuilder.build();
    }

    private GraphAlgebra.IndexPredicate.Triplet protoTriplet(RexCall rexCall) {
        SqlOperator operator = rexCall.getOperator();
        switch (operator.getKind()) {
            case EQUALS:
            case SEARCH:
                RexNode left = rexCall.getOperands().get(0);
                RexNode right = rexCall.getOperands().get(1);
                if (left instanceof RexGraphVariable
                        && (right instanceof RexLiteral || right instanceof RexDynamicParam)) {
                    GraphAlgebra.IndexPredicate.Triplet.Builder tripletBuilder =
                            GraphAlgebra.IndexPredicate.Triplet.newBuilder()
                                    .setKey(protoProperty((RexGraphVariable) left))
                                    .setCmp(
                                            operator.getKind() == SqlKind.EQUALS
                                                    ? OuterExpression.Logical.EQ
                                                    : OuterExpression.Logical.WITHIN);
                    OuterExpression.Expression value =
                            right.accept(
                                    new RexToProtoConverter(
                                            this.deep, this.isColumnId, this.rexBuilder));
                    if (right instanceof RexLiteral) {
                        tripletBuilder.setConst(value.getOperators(0).getConst());
                    } else {
                        tripletBuilder.setParam(value.getOperators(0).getParam());
                    }
                    return tripletBuilder.build();
                } else if (right instanceof RexGraphVariable
                        && (left instanceof RexLiteral || left instanceof RexDynamicParam)) {
                    GraphAlgebra.IndexPredicate.Triplet.Builder tripletBuilder =
                            GraphAlgebra.IndexPredicate.Triplet.newBuilder()
                                    .setKey(protoProperty((RexGraphVariable) right))
                                    .setCmp(
                                            operator.getKind() == SqlKind.EQUALS
                                                    ? OuterExpression.Logical.EQ
                                                    : OuterExpression.Logical.WITHIN);
                    OuterExpression.Expression value =
                            left.accept(
                                    new RexToProtoConverter(
                                            this.deep, this.isColumnId, this.rexBuilder));
                    if (left instanceof RexLiteral) {
                        tripletBuilder.setConst(value.getOperators(0).getConst());
                    } else {
                        tripletBuilder.setParam(value.getOperators(0).getParam());
                    }
                    return tripletBuilder.build();
                }
            default:
                throw new IllegalArgumentException(
                        "can not convert unique key filter pattern="
                                + rexCall
                                + " to ir core index predicate");
        }
    }

    private OuterExpression.Property protoProperty(RexGraphVariable variable) {
        Preconditions.checkArgument(
                variable.getProperty() != null, "cannot convert null property to index predicate");
        return Utils.protoProperty(variable.getProperty());
    }
}
