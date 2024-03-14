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

package com.alibaba.graphscope.common.antlr4;

import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    /**
     * create {@code SqlOperator}(s) according to the given parser trees and opSigns
     * @param trees candidate parser trees
     * @param opSigns for each parser tree which denotes an operator, if it's type is contained in opSigns, then create a {@code SqlOperator} for it
     * @param isPrefix to distinguish unary and binary operators, i.e. unary plus VS binary plus
     * @return
     */
    public static List<SqlOperator> getOperators(
            List<ParseTree> trees, List<String> opSigns, boolean isPrefix) {
        List<SqlOperator> operators = new ArrayList<>();
        for (ParseTree tree : trees) {
            if (tree instanceof TerminalNode && opSigns.contains(tree.getText())) {
                if (tree.getText().equals("+")) {
                    if (isPrefix) {
                        operators.add(GraphStdOperatorTable.UNARY_PLUS);
                    } else {
                        operators.add(GraphStdOperatorTable.PLUS);
                    }
                } else if (tree.getText().equals("-")) {
                    if (isPrefix) {
                        operators.add(GraphStdOperatorTable.UNARY_MINUS);
                    } else {
                        operators.add(GraphStdOperatorTable.MINUS);
                    }
                } else if (tree.getText().equals("*")) {
                    operators.add(GraphStdOperatorTable.MULTIPLY);
                } else if (tree.getText().equals("/")) {
                    operators.add(GraphStdOperatorTable.DIVIDE);
                } else if (tree.getText().equals("%")) {
                    operators.add(GraphStdOperatorTable.MOD);
                } else if (tree.getText().equals("^")) {
                    operators.add(GraphStdOperatorTable.POWER);
                } else if (tree.getText().equals("=")) {
                    operators.add(GraphStdOperatorTable.EQUALS);
                } else if (tree.getText().equals("<>")) {
                    operators.add(GraphStdOperatorTable.NOT_EQUALS);
                } else if (tree.getText().equals("<")) {
                    operators.add(GraphStdOperatorTable.LESS_THAN);
                } else if (tree.getText().equals(">")) {
                    operators.add(GraphStdOperatorTable.GREATER_THAN);
                } else if (tree.getText().equals("<=")) {
                    operators.add(GraphStdOperatorTable.LESS_THAN_OR_EQUAL);
                } else if (tree.getText().equals(">=")) {
                    operators.add(GraphStdOperatorTable.GREATER_THAN_OR_EQUAL);
                } else {
                    throw new UnsupportedOperationException(
                            "operator " + tree.getText() + " is unsupported yet");
                }
            }
        }
        return operators;
    }
}
