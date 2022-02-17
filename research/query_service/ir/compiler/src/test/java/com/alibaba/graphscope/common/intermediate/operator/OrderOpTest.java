/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.intermediate.operator;

import com.alibaba.graphscope.common.utils.FileUtils;
import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.FfiOrderOpt;
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
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(ArgUtils.asFfiNoneVar(), FfiOrderOpt.Asc)), Function.identity()));
        irPlan.appendInterOp(-1, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_asc.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void orderByKeyTest() throws IOException {
        OrderOp op = new OrderOp();
        FfiVariable.ByValue var = ArgUtils.asFfiVar("", "name");
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(var, FfiOrderOpt.Asc)), Function.identity()));
        irPlan.appendInterOp(-1, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_key.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void orderByKeysTest() throws IOException {
        OrderOp op = new OrderOp();
        FfiVariable.ByValue v1 = ArgUtils.asFfiVar("", "name");
        FfiVariable.ByValue v2 = ArgUtils.asFfiVar("", "id");
        op.setOrderVarWithOrder(new OpArg(
                Arrays.asList(Pair.with(v1, FfiOrderOpt.Asc), Pair.with(v2, FfiOrderOpt.Desc)), Function.identity()));
        irPlan.appendInterOp(-1, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_keys.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void orderByLabelTest() throws IOException {
        OrderOp op = new OrderOp();
        FfiVariable.ByValue var = ArgUtils.asFfiVar("", "~label");
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(var, FfiOrderOpt.Asc)), Function.identity()));
        irPlan.appendInterOp(-1, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_label.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void orderLimitTest() throws IOException {
        OrderOp op = new OrderOp();
        FfiVariable.ByValue var = ArgUtils.asFfiNoneVar();
        op.setOrderVarWithOrder(new OpArg(Arrays.asList(Pair.with(var, FfiOrderOpt.Asc)), Function.identity()));
        op.setLower(new OpArg(Integer.valueOf(1), Function.identity()));
        op.setUpper(new OpArg(Integer.valueOf(2), Function.identity()));
        irPlan.appendInterOp(-1, op);
        Assert.assertEquals(FileUtils.readJsonFromResource("order_limit.json"), irPlan.getPlanAsJson());
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }

}
