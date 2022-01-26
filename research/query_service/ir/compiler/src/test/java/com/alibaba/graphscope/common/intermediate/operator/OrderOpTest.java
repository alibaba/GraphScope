package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.utils.FileUtils;
import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.FfiOrderOpt;
import com.alibaba.graphscope.common.jna.type.FfiProperty;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

public class OrderOpTest {
    private IrPlan irPlan = new IrPlan();

    @Test
    public void orderTest() throws IOException {
        OrderOp op = new OrderOp();
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(ArgUtils.asNoneVar(), FfiOrderOpt.Asc)), Function.identity()));
        irPlan.appendInterOp(0, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_asc.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void orderByKeyTest() throws IOException {
        OrderOp op = new OrderOp();
        FfiProperty.ByValue property = ArgUtils.asFfiProperty("name");
        FfiVariable.ByValue var = ArgUtils.asVarPropertyOnly(property);
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(var, FfiOrderOpt.Asc)), Function.identity()));
        irPlan.appendInterOp(0, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_key.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void orderByKeysTest() throws IOException {
        OrderOp op = new OrderOp();
        FfiProperty.ByValue p1 = ArgUtils.asFfiProperty("name");
        FfiVariable.ByValue v1 = ArgUtils.asVarPropertyOnly(p1);
        FfiProperty.ByValue p2 = ArgUtils.asFfiProperty("id");
        FfiVariable.ByValue v2 = ArgUtils.asVarPropertyOnly(p2);
        op.setOrderVarWithOrder(new OpArg(
                Arrays.asList(Pair.with(v1, FfiOrderOpt.Asc), Pair.with(v2, FfiOrderOpt.Desc)), Function.identity()));
        irPlan.appendInterOp(0, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_keys.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void orderByLabelTest() throws IOException {
        OrderOp op = new OrderOp();
        FfiProperty.ByValue property = ArgUtils.asFfiProperty("~label");
        FfiVariable.ByValue var = ArgUtils.asVarPropertyOnly(property);
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(var, FfiOrderOpt.Asc)), Function.identity()));
        irPlan.appendInterOp(0, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_label.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void orderLimitTest() throws IOException {
        OrderOp op = new OrderOp();
        FfiVariable.ByValue var = ArgUtils.asNoneVar();
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(var, FfiOrderOpt.Asc)), Function.identity()));
        op.setLower(new OpArg(Integer.valueOf(1), Function.identity()));
        op.setUpper(new OpArg(Integer.valueOf(2), Function.identity()));
        irPlan.appendInterOp(0, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_limit.json"), irPlan.getPlanAsJson());
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }

}
