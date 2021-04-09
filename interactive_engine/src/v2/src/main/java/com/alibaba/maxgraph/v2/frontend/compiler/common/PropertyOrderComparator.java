package com.alibaba.maxgraph.v2.frontend.compiler.common;

import com.alibaba.maxgraph.proto.v2.OrderType;
import com.alibaba.maxgraph.proto.v2.VariantType;

public class PropertyOrderComparator {
    private String propId;
    private OrderType orderType;
    private VariantType propType;
    private boolean labelFlag;

    public PropertyOrderComparator() {
        this.labelFlag = false;
    }

    public String getPropId() {
        return propId;
    }

    public PropertyOrderComparator setPropId(String propId) {
        this.propId = propId;
        return this;
    }

    public OrderType getOrderType() {
        return orderType;
    }

    public PropertyOrderComparator setOrderType(OrderType orderType) {
        this.orderType = orderType;
        return this;
    }

    public VariantType getPropType() {
        return propType;
    }

    public PropertyOrderComparator setPropType(VariantType propType) {
        this.propType = propType;
        return this;
    }

    public boolean isLabelFlag() {
        return labelFlag;
    }

    public PropertyOrderComparator setLabelFlag(boolean labelFlag) {
        this.labelFlag = labelFlag;
        return this;
    }
}
