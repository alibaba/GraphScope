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
package com.alibaba.maxgraph.compiler.common;

import com.alibaba.maxgraph.Message;

public class PropertyOrderComparator {
    private String propId;
    private Message.OrderType orderType;
    private Message.VariantType propType;
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

    public Message.OrderType getOrderType() {
        return orderType;
    }

    public PropertyOrderComparator setOrderType(Message.OrderType orderType) {
        this.orderType = orderType;
        return this;
    }

    public Message.VariantType getPropType() {
        return propType;
    }

    public PropertyOrderComparator setPropType(Message.VariantType propType) {
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
