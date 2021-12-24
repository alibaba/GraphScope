package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.TestUtils;
import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.*;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;

public class GroupOpTest {
    private IrPlan irPlan = new IrPlan();

    @Test
    public void groupTest() throws IOException {
        GroupOp op = new GroupOp();
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> groupKey = Pair.with(ArgUtils.asNoneVar(), ArgUtils.groupKeysAlias());
        op.setGroupByKeys(new OpArg(Collections.singletonList(groupKey), Function.identity()));

        ArgAggFn aggFn = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.groupValuesAlias());
        op.setGroupByValues(new OpArg(Collections.singletonList(aggFn), Function.identity()));

        irPlan.appendInterOp(op);
        Assert.assertEquals(TestUtils.readJsonFromResource("group.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void groupByKeyTest() throws IOException {
        GroupOp op = new GroupOp();
        FfiProperty.ByValue keyP = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> groupKey = Pair.with((ArgUtils.asVarPropertyOnly(keyP)), ArgUtils.groupKeysAlias());
        op.setGroupByKeys(new OpArg(Collections.singletonList(groupKey), Function.identity()));

        ArgAggFn aggFn = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.groupValuesAlias());
        op.setGroupByValues(new OpArg(Collections.singletonList(aggFn), Function.identity()));

        irPlan.appendInterOp(op);
        Assert.assertEquals(TestUtils.readJsonFromResource("group_key.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void groupByKeyByCountTest() throws IOException {
        GroupOp op = new GroupOp();
        FfiProperty.ByValue keyP = ArgUtils.asFfiProperty("name");
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> groupKey = Pair.with((ArgUtils.asVarPropertyOnly(keyP)), ArgUtils.groupKeysAlias());
        op.setGroupByKeys(new OpArg(Collections.singletonList(groupKey), Function.identity()));

        ArgAggFn aggFn = new ArgAggFn(FfiAggOpt.Count, ArgUtils.groupValuesAlias());
        op.setGroupByValues(new OpArg(Collections.singletonList(aggFn), Function.identity()));

        irPlan.appendInterOp(op);
        Assert.assertEquals(TestUtils.readJsonFromResource("group_key_count.json"), irPlan.getPlanAsJson());
    }


    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}