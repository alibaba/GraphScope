package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.TestUtils;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;

public class DedupOpTest {
    private IrPlan irPlan = new IrPlan();

    @Test
    public void dedupTest() throws IOException {
        DedupOp op = new DedupOp();
        FfiVariable.ByValue dedupKey = ArgUtils.asNoneVar();
        op.setDedupKeys(new OpArg(Collections.singletonList(dedupKey), Function.identity()));

        irPlan.appendInterOp(op);
        Assert.assertEquals(TestUtils.readJsonFromResource("dedup.json"), irPlan.getPlanAsJson());
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}