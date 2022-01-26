package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.utils.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Function;

public class PathExpandOpTest {
    private IrPlan irPlan = new IrPlan();

    @Test
    public void expand_1_5_Test() throws IOException {
        PathExpandOp op = new PathExpandOp();
        op.setEdgeOpt(new OpArg<>(Boolean.valueOf(false), Function.identity()));
        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));
        op.setLower(new OpArg(Integer.valueOf(1), Function.identity()));
        op.setUpper(new OpArg(Integer.valueOf(5), Function.identity()));
        irPlan.appendInterOp(-1, op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("path_expand.json"), actual);
    }
}
