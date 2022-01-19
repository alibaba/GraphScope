package com.alibaba.graphscope.common.intermediate.strategy;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
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
        InterOpCollection opCollection = new InterOpCollection();

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
