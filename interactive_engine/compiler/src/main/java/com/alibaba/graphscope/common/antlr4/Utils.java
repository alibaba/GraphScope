package com.alibaba.graphscope.common.antlr4;

import com.alibaba.graphscope.common.ir.tools.GraphStdOperatorTable;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.List;

public class Utils {
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
