package com.alibaba.graphscope.common.intermediate.calcite;

/**
 * define all kinds of {@code IrOperator}.
 */
public enum IrOperatorKind {
    EXPAND, // for join between entities and relations
    GET_V,
    PATH_EXPAND
}
