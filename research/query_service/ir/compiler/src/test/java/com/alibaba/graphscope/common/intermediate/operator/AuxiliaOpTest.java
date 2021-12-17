package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.TestUtils;
import com.alibaba.graphscope.common.intermediate.AliasArg;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.operator.AuxiliaOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
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
    public void auxiliaPropertiesTest() throws IOException {
        AuxiliaOp op = new AuxiliaOp();
        Set properties = Sets.newHashSet(ArgUtils.strAsNameId("age"), ArgUtils.strAsNameId("name"));
        op.setPropertyDetails(new OpArg(properties, Function.identity()));
        irPlan.appendInterOp(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(TestUtils.readJsonFromResource("auxilia_properties.json"), actual);
    }

    @Test
    public void auxiliaAliasTest() throws IOException {
        AuxiliaOp op = new AuxiliaOp();
        AliasArg alias = new AliasArg(ArgUtils.strAsNameId("a"), true);
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
