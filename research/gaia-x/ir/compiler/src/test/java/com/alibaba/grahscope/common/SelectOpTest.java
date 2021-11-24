package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class SelectOpTest {
    private IrPlan irPlan = new IrPlan();

    @Test
    public void selectOpTest() {
        SelectOp op = new SelectOp();
        op.setPredicate(new OpArg("@.id == 1 && @.name == \"marko\"", Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("select_expr.bytes"), bytes);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
