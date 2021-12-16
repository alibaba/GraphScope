package com.alibaba.graphscope.common;

import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.AuxiliaOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

public class AuxiliaOpTest {
    private IrPlan irPlan = new IrPlan();

    @Test
    public void auxilia_properties_test() throws IOException {
        AuxiliaOp op = new AuxiliaOp();
        Set properties = Sets.newHashSet(ArgUtils.strAsNameId("age"), ArgUtils.strAsNameId("name"));
        op.setPropertyDetails(new OpArg(properties, Function.identity()));
        irPlan.appendInterOp(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(TestUtils.readJsonFromResource("auxilia_properties.json"), actual);
    }

    @Test
    public void auxilia_alias_test() throws IOException {
        AuxiliaOp op = new AuxiliaOp();
        FfiNameOrId.ByValue alias = ArgUtils.strAsNameId("a");
        op.setAlias(new OpArg(alias, Function.identity()));
        irPlan.appendInterOp(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(TestUtils.readJsonFromResource("auxilia_alias.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
