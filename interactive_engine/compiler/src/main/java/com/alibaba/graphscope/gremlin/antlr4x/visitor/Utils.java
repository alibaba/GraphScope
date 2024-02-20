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

package com.alibaba.graphscope.gremlin.antlr4x.visitor;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.javatuples.Pair;

import java.util.List;

public class Utils {
    public static Pair<RexNode, @Nullable String> convertExprToPair(RexNode rex) {
        if (rex.getKind() == SqlKind.AS) {
            List<RexNode> operands = ((RexCall) rex).getOperands();
            return Pair.with(
                    operands.get(0),
                    (String)
                            com.alibaba.graphscope.common.ir.tools.Utils.getValuesAsList(
                                            ((RexLiteral) operands.get(1)).getValue())
                                    .get(0));
        }
        return Pair.with(rex, null);
    }
}
