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

package com.alibaba.grahscope.common;

import com.alibaba.graphscope.common.IrPlan;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.intermediate.operator.SelectOp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class SelectOpTest {
    private IrPlan irPlan = new IrPlan();

    @Test
    public void selectOpTest() {
        SelectOp op = new SelectOp();
        op.setPredicate(new OpArg("@.id == 1 && @.name == \"marko\"", Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("select_expr.bytes"), bytes);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
