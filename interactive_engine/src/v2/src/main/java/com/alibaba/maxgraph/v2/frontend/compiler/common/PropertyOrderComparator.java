/**
 * Copyright 2020 Alibaba Group Holding Limited.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
