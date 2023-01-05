package com.alibaba.graphscope.common.intermediate.core.clause;

import com.alibaba.graphscope.common.intermediate.core.IrNode;
import com.alibaba.graphscope.common.intermediate.core.IrNodeList;
import com.alibaba.graphscope.common.intermediate.core.IrOperatorKind;
import com.alibaba.graphscope.common.intermediate.core.clause.type.OrderOpt;

import org.apache.commons.lang3.NotImplementedException;

/**
 * maintain a list of pairs, each pair consists of expression (variable) and opt (DESC or ASC),
 * and can also be denoted as a {@code IrNode} by using {@link IrOperatorKind#ASC} or {@link IrOperatorKind#DESC}.
 */
public class OrderByClause {
    private IrNodeList orderByList;

    public OrderByClause addOrderBy(IrNode expr, OrderOpt opt) {
        throw new NotImplementedException("");
    }
}
