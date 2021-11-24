package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.operator.LimitOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class LimitOpTest {
    private IrPlan irPlan = new IrPlan();

    @Test
    public void limitOpTest() {
        LimitOp op = new LimitOp();
        op.setLower(new OpArg<>(Integer.valueOf(1), Function.identity()));
        op.setUpper(new OpArg<>(Integer.valueOf(2), Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("limit_range.bytes"), bytes);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
