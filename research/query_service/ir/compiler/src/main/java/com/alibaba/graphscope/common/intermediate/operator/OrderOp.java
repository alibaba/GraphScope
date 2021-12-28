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

package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class OrderOp extends InterOpBase {
    // List of pair<var, orderOpt>
    private Optional<OpArg> orderVarWithOrder;

    // top k
    private Optional<OpArg> lower;

    private Optional<OpArg> upper;

    public OrderOp() {
        super();
        this.orderVarWithOrder = Optional.empty();
        this.lower = Optional.empty();
        this.upper = Optional.empty();
    }

    public Optional<OpArg> getOrderVarWithOrder() {
        return orderVarWithOrder;
    }

    public void setOrderVarWithOrder(OpArg orderVarWithOrder) {
        this.orderVarWithOrder = Optional.of(orderVarWithOrder);
    }

    public Optional<OpArg> getLower() {
        return lower;
    }

    public void setLower(OpArg lower) {
        this.lower = Optional.of(lower);
    }

    public Optional<OpArg> getUpper() {
        return upper;
    }

    public void setUpper(OpArg upper) {
        this.upper = Optional.of(upper);
    }
}
