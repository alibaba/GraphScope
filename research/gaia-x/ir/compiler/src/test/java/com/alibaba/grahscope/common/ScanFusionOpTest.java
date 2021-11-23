package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.OpTransformFactory;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.ScanFusionOp;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiJobBuffer;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.IntByReference;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ScanFusionOpTest {
    private IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;

    @Test
    public void scanOptTest() {
        IntByReference oprIdx = new IntByReference(0);
        Pointer ptrPlan = irCoreLib.initLogicalPlan();
        ScanFusionOp op = new ScanFusionOp(OpTransformFactory.SCAN_FUSION_OP);
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        irCoreLib.appendScanOperator(ptrPlan, (Pointer) op.get(), oprIdx.getValue(), oprIdx);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(GraphStepTest.readBytesFromFile("scan_opt.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    @Test
    public void labelsTest() {
        IntByReference oprIdx = new IntByReference(0);
        Pointer ptrPlan = irCoreLib.initLogicalPlan();
        ScanFusionOp op = new ScanFusionOp(OpTransformFactory.SCAN_FUSION_OP);
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        List<FfiNameOrId.ByValue> values = Arrays.asList(irCoreLib.cstrAsNameOrId("person"));
        op.setLabels(new OpArg<List, List>(values, Function.identity()));
        irCoreLib.appendScanOperator(ptrPlan, (Pointer) op.get(), oprIdx.getValue(), oprIdx);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(GraphStepTest.readBytesFromFile("scan_labels.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }

    @Test
    public void idsTest() {
        IntByReference oprIdx = new IntByReference(0);
        Pointer ptrPlan = irCoreLib.initLogicalPlan();
        ScanFusionOp op = new ScanFusionOp(OpTransformFactory.SCAN_FUSION_OP);
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        List<FfiConst.ByValue> values = Arrays.asList(irCoreLib.int64AsConst(1L));
        op.setIds(new OpArg<List, List>(values, Function.identity()));
        irCoreLib.appendIdxscanOperator(ptrPlan, (Pointer) op.get(), oprIdx.getValue(), oprIdx);
        FfiJobBuffer buffer = irCoreLib.buildPhysicalPlan(ptrPlan);
        Assert.assertArrayEquals(GraphStepTest.readBytesFromFile("scan_ids.bytes"), buffer.getBytes());
        irCoreLib.destroyLogicalPlan(ptrPlan);
    }
}
