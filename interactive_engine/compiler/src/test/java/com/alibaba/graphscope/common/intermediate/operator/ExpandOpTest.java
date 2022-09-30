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
import com.alibaba.graphscope.common.jna.type.FfiDirection;
import com.alibaba.graphscope.common.jna.type.FfiExpandOpt;
import com.alibaba.graphscope.common.utils.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Function;

public class ExpandOpTest {
    private IrPlan irPlan;

    @Test
    public void edgeOptTest() throws IOException {
        ExpandOp op = new ExpandOp();
        op.setEdgeOpt(new OpArg<>(FfiExpandOpt.Edge, Function.identity()));
        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));
        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("expand_edge_opt.json"), actual);
    }

    @Test
    public void labelsTest() throws IOException {
        ExpandOp op = new ExpandOp();
        op.setEdgeOpt(new OpArg<>(FfiExpandOpt.Edge, Function.identity()));
        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));

        QueryParams params = new QueryParams();
        params.addTable(ArgUtils.asNameOrId("knows"));
        op.setParams(params);

        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("expand_labels.json"), actual);
    }

    @Test
    public void aliasTest() throws IOException {
        ExpandOp op = new ExpandOp();
        op.setEdgeOpt(new OpArg<>(FfiExpandOpt.Edge, Function.identity()));
        op.setDirection(new OpArg<>(FfiDirection.Out, Function.identity()));
        op.setAlias(new OpArg(ArgUtils.asAlias("a", true), Function.identity()));
        irPlan = DedupOpTest.getTestIrPlan(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(FileUtils.readJsonFromResource("expand_alias.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
