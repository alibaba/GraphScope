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

package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.ArrayList;
import java.util.List;

public class GraphRexCall extends RexCall {
    public GraphRexCall(RelDataType type, SqlOperator operator, List<? extends RexNode> operands) {
        super(type, operator, operands);
    }

    protected String computeDigest(boolean withType) {
        StringBuilder sb = new StringBuilder(this.op.getName());
        if (this.operands.size() != 0 || this.op.getSyntax() != SqlSyntax.FUNCTION_ID) {
            sb.append("(");
            appendOperands0(sb);
            sb.append(")");
        }

        if (withType) {
            sb.append(":");
            sb.append(this.type.getFullTypeString());
        }

        return sb.toString();
    }

    /**
     * Re-implements the original Calcite behavior that leads to the type mismatch of BIGINT in conditions like 'a.id = 1L'.
     * This type mismatch prevents the query cache from correctly differentiating the hash IDs of conditions such as
     * 'a.id = 1L' and 'a.id = 1', even though they require different execution plans.
     *
     * @param sb The StringBuilder used to construct the query condition.
     */
    private void appendOperands0(StringBuilder sb) {
        if (operands.isEmpty()) {
            return;
        }
        List<String> operandDigests = new ArrayList<>(operands.size());
        for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (!(operand instanceof RexLiteral)
                    || operand.getType().getSqlTypeName() == SqlTypeName.BIGINT) {
                operandDigests.add(operand.toString());
                continue;
            }
            // Type information might be omitted in certain cases to improve readability
            // For instance, AND/OR arguments should be BOOLEAN, so
            // AND(true, null) is better than AND(true, null:BOOLEAN), and we keep the same info.

            // +($0, 2) is better than +($0, 2:BIGINT). Note: if $0 is BIGINT, then 2 is expected to
            // be
            // of BIGINT type as well.
            RexDigestIncludeType includeType = RexDigestIncludeType.OPTIONAL;
            if ((isA(SqlKind.AND) || isA(SqlKind.OR))
                    && operand.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                includeType = RexDigestIncludeType.NO_TYPE;
            }
            if (SqlKind.SIMPLE_BINARY_OPS.contains(getKind())) {
                RexNode otherArg = operands.get(1 - i);
                if ((!(otherArg instanceof RexLiteral) || digestSkipsType((RexLiteral) otherArg))
                        && SqlTypeUtil.equalSansNullability(
                                operand.getType(), otherArg.getType())) {
                    includeType = RexDigestIncludeType.NO_TYPE;
                }
            }
            operandDigests.add(computeDigest((RexLiteral) operand, includeType));
        }
        int totalLength = (operandDigests.size() - 1) * 2; // commas
        for (String s : operandDigests) {
            totalLength += s.length();
        }
        sb.ensureCapacity(sb.length() + totalLength);
        for (int i = 0; i < operandDigests.size(); i++) {
            String op = operandDigests.get(i);
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(op);
        }
    }

    private static boolean digestSkipsType(RexLiteral literal) {
        // This seems trivial, however, this method
        // workarounds https://github.com/typetools/checker-framework/issues/3631
        return literal.digestIncludesType() == RexDigestIncludeType.NO_TYPE;
    }

    private static String computeDigest(RexLiteral literal, RexDigestIncludeType includeType) {
        // This seems trivial, however, this method
        // workarounds https://github.com/typetools/checker-framework/issues/3631
        return literal.computeDigest(includeType);
    }
}
