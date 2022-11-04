package com.alibaba.graphscope.common.intermediate.core;

/**
 * define all kinds of {@code IrOperator}.
 */
public enum IrOperatorKind {
    AS, // for alias
    PLUS, // for arithmetic
    MINUS,
    MULTIPLY,
    DIVIDE,
    EQ, // for logic
    NE,
    GT,
    LT,
    GTE,
    LTE,
    AND,
    OR,
    AGGREGATE_FUNC, // for count, sum, min, max, mean ...
    EXTEND_JOIN, // for join between entities and relations
    DESC, // for opt in order by
    ASC
}
