package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.OpTransformFactory;
import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiJobBuffer;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ExpandOpTest {
    private IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    @Test
    public void edgeOptTest() {
        IntByReference oprIdx = new IntByReference(0);
        Pointer ptrPlan = irCoreLib.initLogicalPlan();
        ExpandOp op = new ExpandOp(OpTransformFactory.EXPAND_OP);
        op.setEdgeOpt(new OpArg<>(Boolean.valueOf(true), Function.identity()));
        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));
        irCoreLib.appendEdgexpdOperator(ptrPlan, (Pointer) op.get(), oprIdx.getValue(), oprIdx);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(GraphStepTest.readBytesFromFile("expand_edge_opt.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
        buffer.close();
    }

    @Test
    public void labelsTest() {
        IntByReference oprIdx = new IntByReference(0);
        Pointer ptrPlan = irCoreLib.initLogicalPlan();
        ExpandOp op = new ExpandOp(OpTransformFactory.EXPAND_OP);
        op.setEdgeOpt(new OpArg<>(Boolean.valueOf(true), Function.identity()));
        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));
        List<FfiNameOrId.ByValue> values = Arrays.asList(irCoreLib.cstrAsNameOrId("knows"));
        op.setLabels(new OpArg<List, List>(values, Function.identity()));
        irCoreLib.appendEdgexpdOperator(ptrPlan, (Pointer) op.get(), oprIdx.getValue(), oprIdx);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(GraphStepTest.readBytesFromFile("expand_labels.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
        buffer.close();
    }
}
