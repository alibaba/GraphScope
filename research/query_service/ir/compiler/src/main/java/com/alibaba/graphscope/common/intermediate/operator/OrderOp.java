package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class OrderOp extends InterOpBase {
    // List of pair<var, orderOpt>
    private Optional<OpArg> orderVarWithOrder;

    public OrderOp() {
        this.orderVarWithOrder = Optional.empty();
    }

    public Optional<OpArg> getOrderVarWithOrder() {
        return orderVarWithOrder;
    }

    public void setOrderVarWithOrder(OpArg orderVarWithOrder) {
        this.orderVarWithOrder = Optional.of(orderVarWithOrder);
    }
}