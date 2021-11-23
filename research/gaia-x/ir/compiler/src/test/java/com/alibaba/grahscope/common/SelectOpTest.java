package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.OpTransformFactory;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiJobBuffer;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class SelectOpTest {
    private IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    @Test
    public void selectOpTest() {
        IntByReference oprIdx = new IntByReference(0);
        Pointer ptrPlan = irCoreLib.initLogicalPlan();
        SelectOp op = new SelectOp(OpTransformFactory.SELECT_OP);
        op.setPredicate(new OpArg("@.id == 1 && @.name == \"marko\"", Function.identity()));
        irCoreLib.appendSelectOperator(ptrPlan, (Pointer) op.get(), oprIdx.getValue(), oprIdx);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(GraphStepTest.readBytesFromFile("select_expr.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
        buffer.close();
    }
}
