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
import com.alibaba.graphscope.common.utils.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Function;

public class SelectOpTest {
    private IrPlan irPlan;

    @Test
    public void selectOpTest() throws IOException {
        SelectOp op = new SelectOp();
        op.setPredicate(new OpArg("@.id == 1 && @.name == \"marko\"", Function.identity()));
        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("select_expr.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
