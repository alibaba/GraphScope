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
import com.alibaba.graphscope.common.TestUtils;
import com.alibaba.graphscope.common.intermediate.AliasArg;
import com.alibaba.graphscope.common.jna.IrCoreLibrary;
import com.alibaba.graphscope.common.jna.type.FfiConst;
import com.alibaba.graphscope.common.jna.type.FfiNameOrId;
import com.alibaba.graphscope.common.jna.type.FfiScanOpt;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ScanFusionOpTest {
    private IrCoreLibrary irCoreLib = IrCoreLibrary.INSTANCE;
    private IrPlan irPlan = new IrPlan();

    @Test
    public void scanOptTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        irPlan.appendInterOp(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(TestUtils.readJsonFromResource("scan_opt.json"), actual);
    }

    @Test
    public void predicateTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        op.setPredicate(new OpArg("@.id == 1", Function.identity()));
        irPlan.appendInterOp(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(TestUtils.readJsonFromResource("scan_expr.json"), actual);
    }

    @Test
    public void labelsTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        List<FfiNameOrId.ByValue> values = Arrays.asList(irCoreLib.cstrAsNameOrId("person"));
        op.setLabels(new OpArg<List, List>(values, Function.identity()));
        irPlan.appendInterOp(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(TestUtils.readJsonFromResource("scan_labels.json"), actual);
    }

    @Test
    public void idsTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        List<FfiConst.ByValue> values = Arrays.asList(irCoreLib.int64AsConst(1L), irCoreLib.int64AsConst(2L));
        op.setIds(new OpArg<List, List>(values, Function.identity()));
        irPlan.appendInterOp(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(TestUtils.readJsonFromResource("scan_ids.json"), actual);
    }

    @Test
    public void aliasTest() throws IOException {
        ScanFusionOp op = new ScanFusionOp();
        op.setScanOpt(new OpArg<>(FfiScanOpt.Vertex, Function.identity()));
        op.setAlias(new OpArg(new AliasArg(irCoreLib.cstrAsNameOrId("a")), Function.identity()));
        irPlan.appendInterOp(op);
        String actual = irPlan.getPlanAsJson();
        Assert.assertEquals(TestUtils.readJsonFromResource("scan_alias.json"), actual);
    }

    @After
    public void after() {
        if (irPlan != null) {
            irPlan.close();
        }
    }
}
