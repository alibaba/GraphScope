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
import com.alibaba.graphscope.common.intermediate.operator.ExpandOp;
import com.alibaba.graphscope.common.intermediate.operator.OpArg;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ExpandOpTest {
    private static IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private IrPlan irPlan = new IrPlan();

    @Test
    public void edgeOptTest() {
        ExpandOp op = new ExpandOp();
        op.setEdgeOpt(new OpArg<>(Boolean.valueOf(true), Function.identity()));
        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("expand_edge_opt.bytes"), bytes);
    }

    @Test
    public void labelsTest() {
        ExpandOp op = new ExpandOp();
        op.setEdgeOpt(new OpArg<>(Boolean.valueOf(true), Function.identity()));
        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));
        List<FfiNameOrId.ByValue> values = Arrays.asList(irCoreLib.cstrAsNameOrId("knows"));
        op.setLabels(new OpArg<List, List>(values, Function.identity()));
        irPlan.appendInterOp(op);
        byte[] bytes = irPlan.toPhysicalBytes();
        Assert.assertArrayEquals(TestUtils.readBytesFromFile("expand_labels.bytes"), bytes);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
