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

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.ArgAggFn;
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.jna.type.*;
import com.alibaba.graphscope.common.utils.FileUtils;

import org.javatuples.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;

public class GroupOpTest {
    private IrPlan irPlan;

    @Test
    public void countTest() throws IOException {
        GroupOp op = new GroupOp();
        op.setGroupByKeys(new OpArg(Collections.emptyList(), Function.identity()));
        ArgAggFn aggFn = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("values", false));
        op.setGroupByValues(new OpArg(Collections.singletonList(aggFn), Function.identity()));

        irPlan = DedupOpTest.getTestIrPlan(op);
        Assert.assertEquals(FileUtils.readJsonFromResource("count.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void countAsTest() throws IOException {
        GroupOp op = new GroupOp();
        op.setGroupByKeys(new OpArg(Collections.emptyList(), Function.identity()));
        ArgAggFn aggFn = new ArgAggFn(FfiAggOpt.Count, ArgUtils.asFfiAlias("a", true));
        op.setGroupByValues(new OpArg(Collections.singletonList(aggFn), Function.identity()));

        irPlan = DedupOpTest.getTestIrPlan(op);
        Assert.assertEquals(
                FileUtils.readJsonFromResource("count_as.json"), irPlan.getPlanAsJson());
    }

    @Test
    public void groupTest() throws IOException {
        GroupOp op = new GroupOp();
        Pair<FfiVariable.ByValue, FfiAlias.ByValue> groupKey =
                Pair.with(ArgUtils.asFfiNoneVar(), ArgUtils.asFfiAlias("keys", false));
        op.setGroupByKeys(new OpArg(Collections.singletonList(groupKey), Function.identity()));

        ArgAggFn aggFn = new ArgAggFn(FfiAggOpt.ToList, ArgUtils.asFfiAlias("values", false));
        op.setGroupByValues(new OpArg(Collections.singletonList(aggFn), Function.identity()));

        irPlan = DedupOpTest.getTestIrPlan(op);
        Assert.assertEquals(FileUtils.readJsonFromResource("group.json"), irPlan.getPlanAsJson());
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
