package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.ScanFusionOp;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ScanFusionOpTest {
    private IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private IrPlan irPlan = new IrPlan();

    @Test
    public void scanOptTest() {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("scan_opt.bytes"), bytes);
    }

    @Test
    public void predicateTest() {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        op.setPredicate(new OpArg("@.id == 1", Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("scan_expr.bytes"), bytes);
    }

    @Test
    public void labelsTest() {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        List<FfiNameOrId.ByValue> values = Arrays.asList(irCoreLib.cstrAsNameOrId("person"));
        op.setLabels(new OpArg<List, List>(values, Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("scan_labels.bytes"), bytes);
    }

    @Test
    public void idsTest() {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        List<FfiConst.ByValue> values = Arrays.asList(irCoreLib.int64AsConst(1L));
        op.setIds(new OpArg<List, List>(values, Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("scan_ids.bytes"), bytes);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
