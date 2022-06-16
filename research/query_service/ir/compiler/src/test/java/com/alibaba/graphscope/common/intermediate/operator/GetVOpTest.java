package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.jna.type.FfiVOpt;
import com.alibaba.graphscope.common.utils.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class GetVOpTest {
    private IrPlan irPlan;

    @Test
    public void getBothVTest() throws IOException {
        GetVOp getVOp = new GetVOp();
        getVOp.setGetVOpt(new OpArg(FfiVOpt.BothV));
        irPlan = DedupOpTest.getTestIrPlan(getVOp);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("get_bothV.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
