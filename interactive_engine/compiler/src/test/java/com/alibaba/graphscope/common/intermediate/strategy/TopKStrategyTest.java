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

package com.alibaba.graphscope.common.intermediate.strategy;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.intermediate.operator.InterOpBase;
import com.alibaba.graphscope.common.intermediate.operator.LimitOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.OrderOp;
import com.alibaba.graphscope.common.jna.type.FfiOrderOpt;

import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class TopKStrategyTest {
    @Test
    public void orderLimitTest() {
        InterOpCollection opCollection = new InterOpCollection(new InterOpBase() {});

        OrderOp orderOp = new OrderOp();
        List<Pair> orderPair = Arrays.asList(Pair.with(ArgUtils.asNoneVar(), FfiOrderOpt.Asc));
        orderOp.setOrderVarWithOrder(new OpArg(orderPair, Function.identity()));
        opCollection.appendInterOp(orderOp);

        LimitOp limitOp = new LimitOp();
        limitOp.setLower(new OpArg<>(Integer.valueOf(1), Function.identity()));
        limitOp.setUpper(new OpArg<>(Integer.valueOf(2), Function.identity()));
        opCollection.appendInterOp(limitOp);

        TopKStrategy.INSTANCE.apply(opCollection);
        OrderOp orderLimit = (OrderOp) opCollection.unmodifiableCollection().get(0);

        Assert.assertEquals(1, opCollection.unmodifiableCollection().size());
        Assert.assertEquals(1, orderLimit.getLower().get().applyArg());
        Assert.assertEquals(2, orderLimit.getUpper().get().applyArg());
    }
}
