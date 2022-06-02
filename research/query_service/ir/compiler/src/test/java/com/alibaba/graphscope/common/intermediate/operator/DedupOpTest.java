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
import com.alibaba.graphscope.common.intermediate.ArgUtils;
import com.alibaba.graphscope.common.intermediate.InterOpCollection;
import com.alibaba.graphscope.common.jna.type.FfiVariable;
import com.alibaba.graphscope.common.store.IrMeta;
import com.alibaba.graphscope.common.utils.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Function;

public class DedupOpTest {
    private IrPlan irPlan;

    @Test
    public void dedupTest() throws IOException {
        DedupOp op = new DedupOp();
        FfiVariable.ByValue dedupKey = ArgUtils.asFfiNoneVar();
        op.setDedupKeys(new OpArg(Collections.singletonList(dedupKey), Function.identity()));

        irPlan = getTestIrPlan(op);
        Assert.assertEquals(FileUtils.readJsonFromResource("dedup.json"), irPlan.getPlanAsJson());
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }

    public static IrPlan getTestIrPlan(InterOpBase op, InterOpBase... ops) {
        InterOpCollection opCollection = new InterOpCollection();
        opCollection.appendInterOp(op);
        if (ops != null) {
            for (InterOpBase op1 : ops) {
                opCollection.appendInterOp(op1);
            }
        }
        return new IrPlan(new IrMeta(""), opCollection);
    }
}
